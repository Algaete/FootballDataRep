using Dapper;
using HtmlAgilityPack;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Playwright;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace CornersMLData.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CornersDataScrappingController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public CornersDataScrappingController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet("scrape-lineups-worldfootball")]
        [ProducesResponseType(typeof(LineupBatchResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> ScrapeLineupsWorldFootball([FromQuery] int take = 5, [FromQuery] int parallelism = 1)
        {
            var connStr = _configuration.GetConnectionString("DefaultConnection");

            if (string.IsNullOrWhiteSpace(connStr))
                return Problem("Connection string 'DefaultConnection' is not configured.");

            if (take <= 0) take = 5;
            if (take > 1000) take = 1000;
            if (parallelism <= 0) parallelism = 1;
            if (parallelism > 4) parallelism = 4;

            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            var pendingList = (await ClaimPendingHomeLineupsAsync(conn, take)).ToList();

            if (!pendingList.Any())
            {
                return Ok(new LineupBatchResponse
                {
                    Message = "No hay partidos pendientes para claim/procesar en WorldFootball.",
                    Total = 0
                });
            }

            using var playwright = await Playwright.CreateAsync();

            var sessionDir = Path.Combine(
                Directory.GetCurrentDirectory(),
                "wwwroot",
                "tmp",
                "playwright-worldfootball-session");

            Directory.CreateDirectory(sessionDir);

            var sharedContext = await playwright.Chromium.LaunchPersistentContextAsync(sessionDir, new BrowserTypeLaunchPersistentContextOptions
            {
                Headless = false,
                SlowMo = 100,
                ViewportSize = new ViewportSize
                {
                    Width = 1600,
                    Height = 1000
                },
                UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                Locale = "en-US",
                Args = new[]
                {
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled"
                }
            });

            var throttler = new SemaphoreSlim(parallelism, parallelism);
            LineupProcessResult[] results;

            try
            {
                var tasks = pendingList
                    .Select((row, index) => ProcessClaimedLineupWithThrottleAsync(
                        sharedContext,
                        connStr,
                        row,
                        index + 1,
                        pendingList.Count,
                        throttler))
                    .ToList();

                results = await Task.WhenAll(tasks);
            }
            finally
            {
                try { await sharedContext.CloseAsync(); } catch { }
                throttler.Dispose();
            }

            return Ok(new LineupBatchResponse
            {
                Message = $"Claimed {pendingList.Count} partidos. Parallelism={parallelism}.",
                Total = results.Length,
                Completed = results.Count(x => x.Status == "COMPLETED"),
                Failed = results.Count(x => x.Status == "FAILED"),
                Errors = results.Count(x => x.Status == "ERROR"),
                Canceled = results.Count(x => x.Status == "CANCELED"),
                Results = results.OrderBy(x => x.Index).ToList()
            });
        }

        private async Task<LineupProcessResult> ProcessClaimedLineupWithThrottleAsync(
            IBrowserContext context,
            string connStr,
            LineupRow row,
            int index,
            int totalBatch,
            SemaphoreSlim throttler)
        {
            await throttler.WaitAsync();

            try
            {
                return await ProcessClaimedLineupAsync(context, connStr, row, index, totalBatch);
            }
            finally
            {
                throttler.Release();
            }
        }

        private async Task<LineupProcessResult> ProcessClaimedLineupAsync(
            IBrowserContext context,
            string connStr,
            LineupRow row,
            int index,
            int totalBatch)
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            IPage? page = null;
            var awayRowInserted = false;

            try
            {
                Console.WriteLine("====================================");
                Console.WriteLine($"PROCESANDO {index}/{totalBatch}");
                Console.WriteLine($"LineupId: {row.LineupId}");
                Console.WriteLine($"MatchId: {row.MatchId}");
                Console.WriteLine($"League: {row.League}");
                Console.WriteLine($"Local: {row.Team}");
                Console.WriteLine($"Visita: {row.Opponent}");
                Console.WriteLine($"Fecha: {row.MatchDate:yyyy-MM-dd}");
                Console.WriteLine("====================================");

                awayRowInserted = await EnsureAwayLineupExists(conn, row);

                page = await context.NewPageAsync();

                var debugFolder = EnsureDebugFolder();
                var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");

                var matchupUrl = await ResolveWorldFootballMatchupUrlAsync(page, row);

                if (string.IsNullOrWhiteSpace(matchupUrl))
                    throw new Exception("No se pudo resolver la URL de matchup en WorldFootball.");

                Console.WriteLine($"MATCHUP URL: {matchupUrl}");

                var lineupUrl = await ResolveLineupUrlFromMatchupPageAsync(page, matchupUrl!, row);

                if (string.IsNullOrWhiteSpace(lineupUrl))
                    throw new Exception("No se pudo resolver la URL /lineup/ del partido.");

                Console.WriteLine($"LINEUP URL: {lineupUrl}");

                var scrape = await ExtractLineupDataFromWorldFootballAsync(page, lineupUrl!, row, debugFolder, stamp);

                var homeStatus = !string.IsNullOrWhiteSpace(scrape.HomeFormation)
                                 || !string.IsNullOrWhiteSpace(scrape.HomeCoach)
                    ? "COMPLETED"
                    : "FAILED";

                var awayStatus = !string.IsNullOrWhiteSpace(scrape.AwayFormation)
                                 || !string.IsNullOrWhiteSpace(scrape.AwayCoach)
                    ? "COMPLETED"
                    : "FAILED";

                var updatedRows = await SaveScrapeResultAsync(conn, row, scrape, lineupUrl!, homeStatus, awayStatus);

                Console.WriteLine($"OK MatchId {row.MatchId}");

                return new LineupProcessResult
                {
                    Index = index,
                    TotalBatch = totalBatch,
                    LineupId = row.LineupId,
                    MatchId = row.MatchId,
                    Team = row.Team,
                    Opponent = row.Opponent,
                    MatchDate = row.MatchDate,
                    MatchupUrl = matchupUrl,
                    LineupUrl = lineupUrl,
                    HomeFormation = scrape.HomeFormation,
                    AwayFormation = scrape.AwayFormation,
                    HomeCoach = scrape.HomeCoach,
                    AwayCoach = scrape.AwayCoach,
                    AllFormations = scrape.AllFormations,
                    AllCoachCandidates = scrape.AllCoachCandidates,
                    DebugHtmlPath = scrape.DebugHtmlPath,
                    DebugImagePath = scrape.DebugImagePath,
                    HomeStatus = homeStatus,
                    AwayStatus = awayStatus,
                    Status = homeStatus == "COMPLETED" || awayStatus == "COMPLETED"
                        ? "COMPLETED"
                        : "FAILED",
                    Database = BuildDatabaseWriteResult(awayRowInserted, updatedRows)
                };
            }
            catch (TaskCanceledException ex)
            {
                Console.WriteLine($"TASK CANCELADA MatchId {row.MatchId}: {ex.Message}");

                var updatedRows = await MarkMatchAsErrorAsync(conn, row.MatchId, "FAILED");

                return new LineupProcessResult
                {
                    Index = index,
                    TotalBatch = totalBatch,
                    LineupId = row.LineupId,
                    MatchId = row.MatchId,
                    Team = row.Team,
                    Opponent = row.Opponent,
                    Error = "Task cancelada por timeout interno de Playwright o navegación.",
                    Detail = ex.Message,
                    Status = "CANCELED",
                    Database = BuildDatabaseWriteResult(awayRowInserted, updatedRows)
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR MatchId {row.MatchId}: {ex}");

                var updatedRows = await MarkMatchAsErrorAsync(conn, row.MatchId, "ERROR");

                return new LineupProcessResult
                {
                    Index = index,
                    TotalBatch = totalBatch,
                    LineupId = row.LineupId,
                    MatchId = row.MatchId,
                    Team = row.Team,
                    Opponent = row.Opponent,
                    Error = ex.Message,
                    Detail = ex.ToString(),
                    Status = "ERROR",
                    Database = BuildDatabaseWriteResult(awayRowInserted, updatedRows)
                };
            }
            finally
            {
                if (page != null)
                {
                    try { await page.CloseAsync(); } catch { }
                }
            }
        }

        private static async Task<List<LineupRow>> ClaimPendingHomeLineupsAsync(SqlConnection conn, int take)
        {
            var rows = await conn.QueryAsync<LineupRow>(@"
                DECLARE @Claimed TABLE (LineupId INT PRIMARY KEY);

                INSERT INTO @Claimed (LineupId)
                SELECT TOP (@Take) LineupId
                FROM dbo.Lineups WITH (UPDLOCK, READPAST, ROWLOCK, INDEX(IX_Lineups_Status))
                WHERE IsHome = 1
                  AND (ScrapingStatus IS NULL OR ScrapingStatus IN ('PENDING', 'FAILED', 'ERROR'))
                ORDER BY
                    MatchDate DESC,
                    LineupId DESC;

                UPDATE L
                SET
                    ScrapingStatus = 'IN_PROGRESS'
                OUTPUT
                    inserted.LineupId,
                    inserted.MatchId,
                    inserted.League,
                    inserted.Season,
                    inserted.MatchDate,
                    inserted.Venue,
                    inserted.Team,
                    inserted.Opponent,
                    inserted.IsHome,
                    inserted.Formation,
                    inserted.Coach,
                    inserted.SourceUrl,
                    inserted.ScrapingStatus
                FROM dbo.Lineups AS L
                INNER JOIN @Claimed AS C
                    ON C.LineupId = L.LineupId;
            ", new { Take = take }, commandTimeout: 60);

            return rows.ToList();
        }

        private static async Task<int> SaveScrapeResultAsync(
            SqlConnection conn,
            LineupRow row,
            WorldFootballLineupResult scrape,
            string sourceUrl,
            string homeStatus,
            string awayStatus)
        {
            return await conn.ExecuteAsync(@"
                UPDATE dbo.Lineups
                SET
                    Formation = @HomeFormation,
                    Coach = @HomeCoach,
                    SourceUrl = @SourceUrl,
                    ScrapingStatus = @HomeStatus,
                    LastScrapedAt = GETDATE()
                WHERE MatchId = @MatchId
                  AND IsHome = 1;

                UPDATE dbo.Lineups
                SET
                    Formation = @AwayFormation,
                    Coach = @AwayCoach,
                    SourceUrl = @SourceUrl,
                    ScrapingStatus = @AwayStatus,
                    LastScrapedAt = GETDATE()
                WHERE MatchId = @MatchId
                  AND IsHome = 0;
            ", new
            {
                MatchId = row.MatchId,
                HomeFormation = NullIfWhite(scrape.HomeFormation),
                AwayFormation = NullIfWhite(scrape.AwayFormation),
                HomeCoach = NullIfWhite(scrape.HomeCoach),
                AwayCoach = NullIfWhite(scrape.AwayCoach),
                SourceUrl = sourceUrl,
                HomeStatus = homeStatus,
                AwayStatus = awayStatus
            }, commandTimeout: 60);
        }

        // =========================================================
        // WORLD FOOTBALL FLOW
        // =========================================================

        private static async Task<string?> ResolveWorldFootballMatchupUrlAsync(IPage page, LineupRow row)
        {
            var directReportUrl = BuildDirectWorldFootballReportUrl(row);

            if (!string.IsNullOrWhiteSpace(directReportUrl))
            {
                Console.WriteLine($"WORLDFOOTBALL DIRECT REPORT URL: {directReportUrl}");
                return directReportUrl;
            }

            var queries = BuildWorldFootballSiteSearchQueries(row);

            foreach (var query in queries)
            {
                Console.WriteLine($"WORLDFOOTBALL SEARCH QUERY: {query}");

                var directMatchReport = await ResolveWorldFootballMatchReportFromSiteSearchAsync(page, row, query);

                if (!string.IsNullOrWhiteSpace(directMatchReport))
                {
                    Console.WriteLine($"WORLDFOOTBALL FIRST RESULT: {directMatchReport}");
                    return directMatchReport;
                }
            }

            return null;
        }

        private static async Task<string?> ResolveWorldFootballMatchReportFromSiteSearchAsync(IPage page, LineupRow row, string query)
        {
            var searchUrl = $"https://www.worldfootball.net/search/list/?q={Uri.EscapeDataString(query)}";

            await page.GotoAsync(searchUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = 45000
            });

            await EnsureWorldFootballVerificationClearedAsync(page);
            await page.WaitForTimeoutAsync(1500);

            var hrefs = await page.EvaluateAsync<string[]>(@"
                () => Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(Boolean)
            ");

            var ordered = hrefs
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Where(x => x.Contains("worldfootball.net", StringComparison.OrdinalIgnoreCase))
                .Where(x =>
                    x.Contains("/match-report/", StringComparison.OrdinalIgnoreCase)
                    || x.Contains("/teams/", StringComparison.OrdinalIgnoreCase))
                .Select(NormalizeSearchResultUrl)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct()
                .OrderByDescending(x => ScoreCandidateUrl(x!, row))
                .ToList();

            if (!ordered.Any())
                await SaveWorldFootballSearchDebugArtifactsAsync(page, query);

            return ordered.FirstOrDefault();
        }

        private static async Task SubmitWorldFootballSiteSearchAsync(IPage page, string query)
        {
            var inputs = page.Locator(
                "form input[type='search'], " +
                "form input[type='text'], " +
                "input[name*='search' i], " +
                "input[name='q'], " +
                "input[placeholder*='search' i], " +
                "input[aria-label*='search' i]");
            var count = await inputs.CountAsync();

            for (var i = 0; i < count; i++)
            {
                var input = inputs.Nth(i);

                try
                {
                    if (!await input.IsVisibleAsync() || !await input.IsEnabledAsync())
                        continue;

                    await input.ClickAsync();
                    await input.FillAsync("");
                    await input.FillAsync(query);
                    await input.PressAsync("Enter");
                    await page.WaitForLoadStateAsync(LoadState.DOMContentLoaded);
                    await page.WaitForTimeoutAsync(1000);
                    return;
                }
                catch
                {
                    // Probamos el siguiente input visible.
                }
            }

            await SaveWorldFootballSearchDebugArtifactsAsync(page, query);
            throw new Exception("No se encontró un input de búsqueda visible en worldfootball.net.");
        }

        private static async Task EnsureWorldFootballVerificationClearedAsync(IPage page, int timeoutMs = 120000)
        {
            if (!page.Url.Contains("worldfootball.net", StringComparison.OrdinalIgnoreCase))
                return;

            async Task<bool> IsVerificationPageAsync()
            {
                try
                {
                    var title = await page.TitleAsync();
                    var text = await page.EvaluateAsync<string>("() => document.body ? document.body.innerText : ''");
                    var html = await page.ContentAsync();

                    return (!string.IsNullOrWhiteSpace(title) && (
                                title.Contains("Just a moment", StringComparison.OrdinalIgnoreCase)
                                || title.Contains("Un momento", StringComparison.OrdinalIgnoreCase)
                                || title.Contains("Moment", StringComparison.OrdinalIgnoreCase)))
                        || (!string.IsNullOrWhiteSpace(text) && (
                                text.Contains("Performing security verification", StringComparison.OrdinalIgnoreCase)
                                || text.Contains("verify you are not a bot", StringComparison.OrdinalIgnoreCase)
                                || text.Contains("security verification", StringComparison.OrdinalIgnoreCase)
                                || text.Contains("Verificación de seguridad en curso", StringComparison.OrdinalIgnoreCase)
                                || text.Contains("Rendimiento y seguridad de Cloudflare", StringComparison.OrdinalIgnoreCase)
                                || text.Contains("Cloudflare", StringComparison.OrdinalIgnoreCase)))
                        || (!string.IsNullOrWhiteSpace(html) && (
                                html.Contains("cf-turnstile-response", StringComparison.OrdinalIgnoreCase)
                                || html.Contains("/cdn-cgi/challenge-platform/", StringComparison.OrdinalIgnoreCase)));
                }
                catch
                {
                    return false;
                }
            }

            if (!await IsVerificationPageAsync())
                return;

            Console.WriteLine("WORLDFOOTBALL: se detectó verificación de seguridad. Resuélvela manualmente en el navegador abierto.");

            var startedAt = DateTime.UtcNow;
            while ((DateTime.UtcNow - startedAt).TotalMilliseconds < timeoutMs)
            {
                await page.WaitForTimeoutAsync(2000);
                if (!await IsVerificationPageAsync())
                {
                    Console.WriteLine("WORLDFOOTBALL: verificación resuelta, continuando.");
                    return;
                }
            }

            throw new Exception("WorldFootball mostró verificación de seguridad (Cloudflare) y no se resolvió dentro del tiempo de espera.");
        }

        private static async Task SaveWorldFootballSearchDebugArtifactsAsync(IPage page, string query)
        {
            try
            {
                var debugFolder = EnsureDebugFolder();
                var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var safeQuery = Regex.Replace(query ?? "query", @"[^a-zA-Z0-9_-]+", "_");
                var htmlPath = Path.Combine(debugFolder, $"worldfootball_search_{safeQuery}_{stamp}.html");
                var pngPath = Path.Combine(debugFolder, $"worldfootball_search_{safeQuery}_{stamp}.png");
                var html = await page.ContentAsync();

                await System.IO.File.WriteAllTextAsync(htmlPath, html);
                await page.ScreenshotAsync(new PageScreenshotOptions
                {
                    Path = pngPath,
                    FullPage = true
                });

                Console.WriteLine($"WORLDFOOTBALL SEARCH DEBUG HTML: {htmlPath}");
                Console.WriteLine($"WORLDFOOTBALL SEARCH DEBUG PNG: {pngPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WORLDFOOTBALL SEARCH DEBUG ERROR: {ex.Message}");
            }
        }

        private static List<string> BuildWorldFootballSiteSearchQueries(LineupRow row)
        {
            var dateIso = row.MatchDate?.ToString("yyyy-MM-dd") ?? "";
            var dateAlt = row.MatchDate?.ToString("dd.MM.yyyy") ?? "";
            var team = NormalizeForSearch(row.Team);
            var opp = NormalizeForSearch(row.Opponent);
            var league = NormalizeForSearch(row.League);
            var season = NormalizeForSearch(row.Season);

            var list = new List<string>
            {
                $"{team} {opp} {dateAlt}",
                $"{team} {opp} {dateIso}",
                $"{team} {opp} {league} {season}",
                $"{team} {opp} {league}",
                $"{team} {opp}"
            };

            return list.Distinct().ToList();
        }

        private static int ScoreCandidateUrl(string url, LineupRow row)
        {
            if (string.IsNullOrWhiteSpace(url))
                return 0;

            var score = 0;
            var normalizedUrl = RemoveDiacritics(url).ToLowerInvariant();
            var team = RemoveDiacritics((row.Team ?? "").ToLowerInvariant());
            var opp = RemoveDiacritics((row.Opponent ?? "").ToLowerInvariant());

            foreach (var token in TokenizeTeamName(team))
            {
                if (normalizedUrl.Contains(token))
                    score += 4;
            }

            foreach (var token in TokenizeTeamName(opp))
            {
                if (normalizedUrl.Contains(token))
                    score += 4;
            }

            var league = RemoveDiacritics((row.League ?? "").ToLowerInvariant()).Replace(" ", "-");

            if (normalizedUrl.Contains("worldfootball.net")) score += 10;
            if (normalizedUrl.Contains("/match-report/")) score += 30;
            if (normalizedUrl.Contains("/lineup/")) score += 25;
            if (normalizedUrl.Contains("/matches-against/")) score += 12;
            if (normalizedUrl.Contains("/teams/")) score += 8;
            if (!string.IsNullOrWhiteSpace(league) && normalizedUrl.Contains(league)) score += 8;

            return score;
        }

        private static async Task<string?> ResolveLineupUrlFromMatchupPageAsync(IPage page, string matchupOrMatchReportUrl, LineupRow row)
        {
            // Si ya es match-report, forzamos directo a /lineup/
            if (matchupOrMatchReportUrl.Contains("/match-report/", StringComparison.OrdinalIgnoreCase))
            {
                return ToLineupUrl(matchupOrMatchReportUrl);
            }

            if (matchupOrMatchReportUrl.Contains("/report/", StringComparison.OrdinalIgnoreCase))
            {
                return ToLineupUrl(matchupOrMatchReportUrl);
            }

            await page.GotoAsync(matchupOrMatchReportUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = 30000
            });

            await EnsureWorldFootballVerificationClearedAsync(page);
            await page.WaitForTimeoutAsync(2500);

            // Busca fila por fecha visible.
            var targetDate1 = row.MatchDate?.ToString("dd.MM.yyyy") ?? "";
            var targetDate2 = row.MatchDate?.ToString("d.M.yyyy") ?? "";

            // Primero intentamos por fecha exacta y luego clic al score dentro de esa fila.
            var lineupUrl = await page.EvaluateAsync<string?>(@"
                ({ targetDate1, targetDate2 }) => {
                    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
                    const rows = Array.from(document.querySelectorAll('tr'));

                    for (const tr of rows) {
                        const text = clean(tr.innerText);

                        if (!text) continue;
                        if (!text.includes(targetDate1) && !text.includes(targetDate2)) continue;

                        const links = Array.from(tr.querySelectorAll('a[href]'));

                        // Preferimos links a match-report o links que parezcan ser el resultado.
                        let candidate =
                            links.find(a => a.href.includes('/match-report/')) ||
                            links.find(a => /\d+:\d+/.test(clean(a.textContent))) ||
                            links[0];

                        if (candidate && candidate.href) {
                            return candidate.href;
                        }
                    }

                    return null;
                }
            ", new { targetDate1, targetDate2 });

            if (string.IsNullOrWhiteSpace(lineupUrl))
            {
                // Fallback: si hay muchos match-report links, elegimos el más cercano por tokens.
                var links = await page.EvaluateAsync<string[]>(@"
                    () => Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h => h && h.includes('/match-report/'))
                        .slice(0, 100)
                ");

                var best = links
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Distinct()
                    .OrderByDescending(x => ScoreCandidateUrl(x, row))
                    .FirstOrDefault();

                lineupUrl = best;
            }

            return string.IsNullOrWhiteSpace(lineupUrl) ? null : ToLineupUrl(lineupUrl);
        }

        private static string ToLineupUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
                return url;

            var clean = url.Split('?')[0].Split('#')[0];

            if (clean.EndsWith("/lineup/", StringComparison.OrdinalIgnoreCase))
                return clean;

            if (clean.EndsWith("/head-to-head/", StringComparison.OrdinalIgnoreCase))
                return clean.Replace("/head-to-head/", "/lineup/", StringComparison.OrdinalIgnoreCase);

            if (clean.EndsWith("/standings/", StringComparison.OrdinalIgnoreCase))
                return clean.Replace("/standings/", "/lineup/", StringComparison.OrdinalIgnoreCase);

            if (clean.Contains("/match-report/", StringComparison.OrdinalIgnoreCase))
            {
                if (!clean.EndsWith("/"))
                    clean += "/";

                return clean + "lineup/";
            }

            if (clean.Contains("/report/", StringComparison.OrdinalIgnoreCase))
            {
                if (!clean.EndsWith("/"))
                    clean += "/";

                return clean + "lineup/";
            }

            return clean;
        }

        private static async Task<WorldFootballLineupResult> ExtractLineupDataFromWorldFootballAsync(
            IPage page,
            string lineupUrl,
            LineupRow row,
            string debugFolder,
            string stamp)
        {
            await page.GotoAsync(lineupUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = 30000
            });

            await EnsureWorldFootballVerificationClearedAsync(page);
            await page.WaitForTimeoutAsync(2500);

            var html = await page.ContentAsync();
            var visibleText = await page.EvaluateAsync<string>("() => document.body ? document.body.innerText : ''");

            var allFormations = ExtractValidFormations(visibleText)
                .Concat(ExtractValidFormations(html))
                .Distinct()
                .ToList();

            // Intento 1: sacar team + formation desde headings/títulos visibles tipo:
            // Bor. Mönchengladbach (4-2-3-1)
            // Werder Bremen (3-4-2-1)
            var teamFormationMap = await page.EvaluateAsync<Dictionary<string, string>>(@"
                () => {
                    const map = {};
                    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
                    const regex = /(.*)\((\d(?:-\d){2,4})\)/;

                    const nodes = Array.from(document.querySelectorAll('h1,h2,h3,h4,strong,b,td,th,div,span,a'));

                    for (const node of nodes) {
                        const txt = clean(node.textContent);
                        if (!txt) continue;

                        const m = txt.match(regex);
                        if (!m) continue;

                        const team = clean(m[1]);
                        const formation = clean(m[2]);

                        if (team && formation && !map[team]) {
                            map[team] = formation;
                        }
                    }

                    return map;
                }
            ");

            string? homeFormation = null;
            string? awayFormation = null;

            if (teamFormationMap != null && teamFormationMap.Any())
            {
                homeFormation = MatchFormationByTeamName(row.Team, teamFormationMap);
                awayFormation = MatchFormationByTeamName(row.Opponent, teamFormationMap);
            }

            // Intento 2: fallback por orden si no matcheó por nombre
            if (string.IsNullOrWhiteSpace(homeFormation))
                homeFormation = allFormations.ElementAtOrDefault(0);

            if (string.IsNullOrWhiteSpace(awayFormation))
                awayFormation = allFormations.ElementAtOrDefault(1);

            // Coaches
            var coachCandidates = ExtractCoachCandidates(visibleText)
                .Concat(ExtractCoachCandidates(html))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            string? homeCoach = null;
            string? awayCoach = null;

            homeCoach = ExtractManagerFromHtml(html, "home");
            awayCoach = ExtractManagerFromHtml(html, "away");

            // Fallback simple por orden de aparición si no logramos aislar home/away
            if (coachCandidates.Count > 0)
                homeCoach ??= coachCandidates.ElementAtOrDefault(0);

            if (coachCandidates.Count > 1)
                awayCoach ??= coachCandidates.ElementAtOrDefault(1);

            var htmlPath = Path.Combine(debugFolder, $"worldfootball_lineup_{row.LineupId}_{stamp}.html");
            var pngPath = Path.Combine(debugFolder, $"worldfootball_lineup_{row.LineupId}_{stamp}.png");

            await System.IO.File.WriteAllTextAsync(htmlPath, html);

            try
            {
                await page.ScreenshotAsync(new PageScreenshotOptions
                {
                    Path = pngPath,
                    FullPage = true,
                    Timeout = 10000
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SCREENSHOT DEBUG FAILED: {ex.Message}");
            }

            return new WorldFootballLineupResult
            {
                HomeFormation = homeFormation,
                AwayFormation = awayFormation,
                HomeCoach = homeCoach,
                AwayCoach = awayCoach,
                AllFormations = allFormations,
                AllCoachCandidates = coachCandidates,
                DebugHtmlPath = htmlPath,
                DebugImagePath = pngPath
            };
        }

        // =========================================================
        // HELPERS
        // =========================================================

        private static string? NormalizeSearchResultUrl(string? url)
        {
            if (string.IsNullOrWhiteSpace(url))
                return null;

            if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
                return url;

            if (uri.Host.Contains("google.", StringComparison.OrdinalIgnoreCase)
                && uri.AbsolutePath.Equals("/url", StringComparison.OrdinalIgnoreCase))
            {
                var query = System.Web.HttpUtility.ParseQueryString(uri.Query);
                return query["q"] ?? query["url"] ?? url;
            }

            return url;
        }

        private static async Task<bool> EnsureAwayLineupExists(SqlConnection conn, LineupRow homeRow)
        {
            var insertedRows = await conn.ExecuteAsync(@"
                IF NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.Lineups
                    WHERE MatchId = @MatchId
                      AND IsHome = 0
                )
                BEGIN
                    INSERT INTO dbo.Lineups
                    (
                        MatchId,
                        League,
                        Season,
                        MatchDate,
                        Venue,
                        Team,
                        Opponent,
                        IsHome,
                        Formation,
                        Coach,
                        StartingXIJson,
                        BenchJson,
                        SourceUrl,
                        ScrapingStatus,
                        LastScrapedAt,
                        CreatedAt
                    )
                    VALUES
                    (
                        @MatchId,
                        @League,
                        @Season,
                        @MatchDate,
                        @Venue,
                        @AwayTeam,
                        @HomeTeam,
                        0,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        'PENDING',
                        NULL,
                        SYSDATETIME()
                    );
                END
            ", new
            {
                homeRow.MatchId,
                homeRow.League,
                homeRow.Season,
                homeRow.MatchDate,
                homeRow.Venue,
                AwayTeam = homeRow.Opponent,
                HomeTeam = homeRow.Team
            }, commandTimeout: 30);

            return insertedRows > 0;
        }

        private static async Task<int> MarkMatchAsErrorAsync(SqlConnection conn, int? matchId, string status)
        {
            return await conn.ExecuteAsync(@"
                UPDATE dbo.Lineups
                SET
                    ScrapingStatus = @Status,
                    LastScrapedAt = GETDATE()
                WHERE MatchId = @MatchId;
            ", new
            {
                MatchId = matchId,
                Status = status
            }, commandTimeout: 30);
        }

        private static DatabaseWriteResult BuildDatabaseWriteResult(bool awayRowInserted, int rowsAffected)
        {
            return new DatabaseWriteResult
            {
                AwayRowInserted = awayRowInserted,
                RowsAffected = rowsAffected,
                HomeRowUpdated = rowsAffected >= 1,
                AwayRowUpdated = rowsAffected >= 2,
                Persisted = rowsAffected > 0
            };
        }

        private static string EnsureDebugFolder()
        {
            var folder = Path.Combine(
                Directory.GetCurrentDirectory(),
                "wwwroot",
                "tmp",
                "lineups-debug"
            );

            Directory.CreateDirectory(folder);
            return folder;
        }

        private static string? NullIfWhite(string? value)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
        }

        private static string NormalizeForSearch(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "";

            var s = RemoveDiacritics(value)
                .Replace(".", " ")
                .Replace("-", " ")
                .Replace("_", " ")
                .Trim();

            s = Regex.Replace(s, @"\s+", " ");

            return s;
        }

        private static string? BuildDirectWorldFootballReportUrl(LineupRow row)
        {
            var competitionSlug = MapCompetitionToWorldFootballReportSlug(row.League);
            if (string.IsNullOrWhiteSpace(competitionSlug))
                return null;

            var homeSlug = MapTeamToWorldFootballReportSlug(row.Team);
            var awaySlug = MapTeamToWorldFootballReportSlug(row.Opponent);
            var seasonSlug = BuildSeasonSlug(row);

            if (string.IsNullOrWhiteSpace(homeSlug)
                || string.IsNullOrWhiteSpace(awaySlug)
                || string.IsNullOrWhiteSpace(seasonSlug))
                return null;

            return $"https://www.worldfootball.net/report/{competitionSlug}-{seasonSlug}-{homeSlug}-{awaySlug}/";
        }

        private static string? MapCompetitionToWorldFootballReportSlug(string? league)
        {
            var key = NormalizeTeamKey(league ?? "");

            return key switch
            {
                "bundesliga" => "bundesliga",
                "2 bundesliga" => "2-bundesliga",
                "dfb pokal" => "dfb-pokal",
                "serie a" => "serie-a",
                "serie b" => "serie-b",
                "coppa italia" => "coppa-italia",
                "ligue 1" => "ligue-1",
                _ => null
            };
        }

        private static string? BuildSeasonSlug(LineupRow row)
        {
            var season = row.Season?.Trim();

            if (!string.IsNullOrWhiteSpace(season))
            {
                var digits = Regex.Match(season, @"(?<start>\d{4}).*?(?<end>\d{4})");
                if (digits.Success)
                    return $"{digits.Groups["start"].Value}-{digits.Groups["end"].Value}";

                if (Regex.IsMatch(season, @"^\d{4}/\d{2}$"))
                {
                    var start = season.Substring(0, 4);
                    var end = $"20{season.Substring(5, 2)}";
                    return $"{start}-{end}";
                }
            }

            if (!row.MatchDate.HasValue)
                return null;

            var matchDate = row.MatchDate.Value;
            var startYear = matchDate.Month >= 7 ? matchDate.Year : matchDate.Year - 1;
            var endYear = startYear + 1;
            return $"{startYear}-{endYear}";
        }

        private static string? MapTeamToWorldFootballReportSlug(string? team)
        {
            if (string.IsNullOrWhiteSpace(team))
                return null;

            var key = NormalizeTeamKey(team);

            return key switch
            {
                "alemannia aachen" => "alemannia-aachen",
                "ac milan" => "ac-milan",
                "arminia bielefeld" => "arminia-bielefeld",
                "angers" => "angers-sco",
                "as roma" => "as-roma",
                "atalanta" => "atalanta-bc",
                "auxerre" => "aj-auxerre",
                "augsburg" => "fc-augsburg",
                "avellino" => "us-avellino-1912",
                "bari" => "ssc-bari",
                "b monchengladbach" => "bor-moenchengladbach",
                "bor monchengladbach" => "bor-moenchengladbach",
                "monchengladbach" => "bor-moenchengladbach",
                "bayer leverkusen" => "bayer-leverkusen",
                "bayern munich" => "bayern-munich",
                "bologna" => "bologna-fc-1909",
                "bochum" => "vfl-bochum",
                "braunschweig" => "eintracht-braunschweig",
                "bremen" => "bremer-sv",
                "brescia" => "brescia-calcio",
                "brest" => "stade-brestois-29",
                "cottbus" => "energie-cottbus",
                "carrarese" => "carrarese-calcio",
                "catanzaro" => "us-catanzaro",
                "cesena" => "cesena-fc",
                "cagliari" => "cagliari-calcio",
                "cittadella" => "as-cittadella",
                "como" => "como-1907",
                "cosenza" => "cosenza-calcio",
                "cremonese" => "us-cremonese",
                "darmstadt" => "sv-darmstadt-98",
                "dortmund" => "borussia-dortmund",
                "duisburg" => "msv-duisburg",
                "dusseldorf" => "fortuna-duesseldorf",
                "eintracht frankfurt" => "eintracht-frankfurt",
                "elversberg" => "sv-07-elversberg",
                "empoli" => "empoli-fc",
                "fc koln" => "1-fc-koeln",
                "fc cologne" => "1-fc-koeln",
                "fiorentina" => "acf-fiorentina",
                "freiburg" => "sc-freiburg",
                "frosinone" => "frosinone-calcio",
                "genoa" => "genoa-cfc",
                "greifswald" => "greifswalder-fc",
                "greuther furth" => "spvgg-greuther-fuerth",
                "greuther fuerth" => "spvgg-greuther-fuerth",
                "hallescher" => "hallescher-fc",
                "hamburger sv" => "hamburger-sv",
                "hannover" => "hannover-96",
                "hansa rostock" => "hansa-rostock",
                "heidenheim" => "1-fc-heidenheim-1846",
                "hertha berlin" => "hertha-bsc",
                "hildesheim" => "vfv-06-hildesheim",
                "holstein kiel" => "holstein-kiel",
                "hoffenheim" => "1899-hoffenheim",
                "ingolstadt" => "fc-ingolstadt-04",
                "inter" => "inter",
                "juve stabia" => "ss-juve-stabia",
                "juventus" => "juventus",
                "kaiserslautern" => "1-fc-kaiserslautern",
                "karlsruher sc" => "karlsruher-sc",
                "lazio" => "lazio-rom",
                "le havre" => "le-havre-ac",
                "lecce" => "us-lecce",
                "lens" => "rc-lens",
                "lille" => "losc-lille",
                "lotte" => "sportfreunde-lotte",
                "lyon" => "olympique-lyonnais",
                "mainz" => "1-fsv-mainz-05",
                "magdeburg" => "1-fc-magdeburg",
                "mantova" => "mantova-1911",
                "marseille" => "olympique-marseille",
                "meppen" => "sv-meppen",
                "monaco" => "as-monaco",
                "modena" => "modena-fc-2018",
                "monza" => "ac-monza",
                "montpellier" => "montpellier-hsc",
                "munich 1860" => "1860-muenchen",
                "1860 munich" => "1860-muenchen",
                "nantes" => "fc-nantes",
                "napoli" => "ssc-napoli",
                "nice" => "ogc-nice",
                "nurnberg" => "1-fc-nuernberg",
                "offenbach" => "kickers-offenbach",
                "paderborn" => "sc-paderborn-07",
                "palermo" => "palermo-fc",
                "parma" => "parma-calcio-1913",
                "phonix lubeck" => "1-fc-phoenix-luebeck",
                "phoenix lubeck" => "1-fc-phoenix-luebeck",
                "pisa" => "pisa-sc",
                "preussen munster" => "sc-preussen-muenster",
                "psg" => "paris-saint-germain",
                "rb leipzig" => "rb-leipzig",
                "reggiana" => "ac-reggiana-1919",
                "regensburg" => "jahn-regensburg",
                "reims" => "stade-reims",
                "rennes" => "stade-rennais",
                "rw essen" => "rot-weiss-essen",
                "saarbrucken" => "1-fc-saarbruecken",
                "salernitana" => "us-salernitana-1919",
                "sandhausen" => "sv-sandhausen",
                "sampdoria" => "uc-sampdoria",
                "sassuolo" => "us-sassuolo-calcio",
                "schalke" => "fc-schalke-04",
                "schott mainz" => "tsv-schott-mainz",
                "sg dynamo dresden" => "dynamo-dresden",
                "spezia" => "spezia-calcio",
                "st pauli" => "fc-st-pauli",
                "st etienne" => "as-st-etienne",
                "strasbourg" => "rc-strasbourg",
                "stuttgart" => "vfb-stuttgart",
                "sudtirol" => "fc-suedtirol",
                "teutonia ottensen" => "teutonia-ottensen",
                "torino" => "torino-fc",
                "torres" => "se-torres-1903",
                "toulouse" => "fc-toulouse",
                "union berlin" => "1-fc-union-berlin",
                "unterhaching" => "spvgg-unterhaching",
                "udinese" => "udinese-calcio",
                "ulm" => "ssv-ulm-1846",
                "venezia" => "venezia-fc",
                "verona" => "hellas-verona",
                "vfl osnabruck" => "vfl-osnabrueck",
                "viktoria berlin" => "fc-viktoria-1889-berlin",
                "villingen" => "fc-08-villingen",
                "wehen" => "sv-wehen-wiesbaden",
                "werder bremen" => "werder-bremen",
                "wolfsburg" => "vfl-wolfsburg",
                "wurzburger kickers" => "wuerzburger-kickers",
                _ => null
            };
        }

        private static string NormalizeTeamKey(string value)
        {
            var normalized = RemoveDiacritics(value)
                .ToLowerInvariant()
                .Replace(".", " ")
                .Replace("-", " ");

            normalized = Regex.Replace(normalized, @"\s+", " ").Trim();
            normalized = Regex.Replace(normalized, @"\b[23]$", "").Trim();
            return normalized;
        }

        private static string RemoveDiacritics(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return text;

            var normalized = text.Normalize(NormalizationForm.FormD);
            var chars = normalized
                .Where(c => CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark)
                .ToArray();

            return new string(chars).Normalize(NormalizationForm.FormC);
        }

        private static List<string> TokenizeTeamName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return new List<string>();

            var tokens = Regex.Split(name, @"[^a-z0-9]+", RegexOptions.IgnoreCase)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Where(x => x.Length >= 3)
                .Distinct()
                .ToList();

            return tokens;
        }

        private static List<string> ExtractValidFormations(string? text)
        {
            var list = new List<string>();

            if (string.IsNullOrWhiteSpace(text))
                return list;

            var matches = Regex.Matches(text, @"\b\d(?:-\d){2,4}\b");

            foreach (Match m in matches)
            {
                if (IsValidFormation(m.Value) && !list.Contains(m.Value))
                    list.Add(m.Value);
            }

            return list;
        }

        private static bool IsValidFormation(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return false;

            value = value.Trim();

            if (!Regex.IsMatch(value, @"^\d(?:-\d){2,4}$"))
                return false;

            var parts = value.Split('-');

            if (parts.Length < 3 || parts.Length > 5)
                return false;

            var sum = 0;

            foreach (var part in parts)
            {
                if (!int.TryParse(part, out var n))
                    return false;

                if (n < 1 || n > 6)
                    return false;

                sum += n;
            }

            return sum == 10;
        }

        private static string? MatchFormationByTeamName(string? teamName, Dictionary<string, string> map)
        {
            if (string.IsNullOrWhiteSpace(teamName) || map == null || !map.Any())
                return null;

            var normalizedTeam = RemoveDiacritics(teamName).ToLowerInvariant();

            // Exact-ish
            foreach (var kv in map)
            {
                var key = RemoveDiacritics(kv.Key).ToLowerInvariant();

                if (key.Contains(normalizedTeam) || normalizedTeam.Contains(key))
                    return kv.Value;
            }

            // Token score
            var best = map
                .Select(kv => new
                {
                    kv.Value,
                    Score = TokenizeTeamName(normalizedTeam)
                        .Count(t => RemoveDiacritics(kv.Key).ToLowerInvariant().Contains(t))
                })
                .OrderByDescending(x => x.Score)
                .FirstOrDefault();

            return best != null && best.Score > 0 ? best.Value : null;
        }

        private static List<string> ExtractCoachCandidates(string? text)
        {
            var list = new List<string>();

            if (string.IsNullOrWhiteSpace(text))
                return list;

            var regexes = new[]
            {
                @"(?:Coach|Manager|Trainer)\s*:?\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\.\-'\s]{2,80})",
                @"(?:Head coach|Team manager)\s*:?\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\.\-'\s]{2,80})"
            };

            foreach (var pattern in regexes)
            {
                var matches = Regex.Matches(text, pattern, RegexOptions.IgnoreCase);

                foreach (Match match in matches)
                {
                    if (match.Groups.Count < 2) continue;

                    var value = match.Groups[1].Value?.Trim();

                    if (string.IsNullOrWhiteSpace(value)) continue;
                    if (value.Length < 3) continue;
                    if (!list.Contains(value, StringComparer.OrdinalIgnoreCase))
                        list.Add(value);
                }
            }

            return list;
        }

        private static string? ExtractManagerFromHtml(string? html, string side)
        {
            if (string.IsNullOrWhiteSpace(html) || string.IsNullOrWhiteSpace(side))
                return null;

            try
            {
                var doc = new HtmlDocument();
                doc.LoadHtml(html);

                var coachNodes = doc.DocumentNode.SelectNodes("//div[contains(@class,'hs-lineup--coaches')]");
                if (coachNodes == null || coachNodes.Count == 0)
                    return null;

                foreach (var node in coachNodes)
                {
                    var classes = node.GetAttributeValue("class", "");
                    if (!classes.Contains(side, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var children = node.ChildNodes
                        .Where(x => x.NodeType == HtmlNodeType.Element)
                        .ToList();

                    for (var i = 0; i < children.Count; i++)
                    {
                        var child = children[i];
                        var childClasses = child.GetAttributeValue("class", "");
                        var role = HtmlEntity.DeEntitize(child.InnerText ?? "").Trim();

                        if (!childClasses.Contains("role", StringComparison.OrdinalIgnoreCase))
                            continue;

                        if (!Regex.IsMatch(role, @"^(Coach|Manager|Trainer)$", RegexOptions.IgnoreCase))
                            continue;

                        for (var j = i + 1; j < children.Count; j++)
                        {
                            var next = children[j];
                            var nextClasses = next.GetAttributeValue("class", "");
                            if (nextClasses.Contains("role", StringComparison.OrdinalIgnoreCase))
                                break;

                            var link = next.SelectSingleNode(".//a");
                            var text = HtmlEntity.DeEntitize(link?.InnerText ?? next.InnerText ?? "").Trim();
                            text = Regex.Replace(text, @"\s+", " ");

                            if (!string.IsNullOrWhiteSpace(text))
                                return text;
                        }
                    }
                }
            }
            catch
            {
                // Best effort: si el HTML cambia, seguimos con los fallbacks.
            }

            return null;
        }

        // =========================================================
        // DTOs
        // =========================================================

        private class WorldFootballLineupResult
        {
            public string? HomeFormation { get; set; }
            public string? AwayFormation { get; set; }
            public string? HomeCoach { get; set; }
            public string? AwayCoach { get; set; }
            public List<string> AllFormations { get; set; } = new();
            public List<string> AllCoachCandidates { get; set; } = new();
            public string? DebugHtmlPath { get; set; }
            public string? DebugImagePath { get; set; }
        }

        private class LineupRow
        {
            public int LineupId { get; set; }
            public int? MatchId { get; set; }
            public string? League { get; set; }
            public string? Season { get; set; }
            public DateTime? MatchDate { get; set; }
            public string? Venue { get; set; }
            public string? Team { get; set; }
            public string? Opponent { get; set; }
            public bool? IsHome { get; set; }
            public string? Formation { get; set; }
            public string? Coach { get; set; }
            public string? SourceUrl { get; set; }
            public string? ScrapingStatus { get; set; }
        }

        public class LineupBatchResponse
        {
            public string? Message { get; set; }
            public int Total { get; set; }
            public int Completed { get; set; }
            public int Failed { get; set; }
            public int Errors { get; set; }
            public int Canceled { get; set; }
            public List<LineupProcessResult> Results { get; set; } = new();
        }

        public class LineupProcessResult
        {
            public int Index { get; set; }
            public int TotalBatch { get; set; }
            public int LineupId { get; set; }
            public int? MatchId { get; set; }
            public string? Team { get; set; }
            public string? Opponent { get; set; }
            public DateTime? MatchDate { get; set; }
            public string? MatchupUrl { get; set; }
            public string? LineupUrl { get; set; }
            public string? HomeFormation { get; set; }
            public string? AwayFormation { get; set; }
            public string? HomeCoach { get; set; }
            public string? AwayCoach { get; set; }
            public List<string> AllFormations { get; set; } = new();
            public List<string> AllCoachCandidates { get; set; } = new();
            public string? DebugHtmlPath { get; set; }
            public string? DebugImagePath { get; set; }
            public string? HomeStatus { get; set; }
            public string? AwayStatus { get; set; }
            public string Status { get; set; } = "";
            public string? Error { get; set; }
            public string? Detail { get; set; }
            public DatabaseWriteResult Database { get; set; } = new();
        }

        public class DatabaseWriteResult
        {
            public bool AwayRowInserted { get; set; }
            public int RowsAffected { get; set; }
            public bool HomeRowUpdated { get; set; }
            public bool AwayRowUpdated { get; set; }
            public bool Persisted { get; set; }
        }
    }
}
