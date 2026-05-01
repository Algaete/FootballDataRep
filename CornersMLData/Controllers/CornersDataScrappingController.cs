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
        private static readonly TimeSpan WorldFootballCooldownWindow = TimeSpan.FromMinutes(5);
        private static readonly object WorldFootballCooldownLock = new();
        private static DateTimeOffset? _worldFootballCooldownUntilUtc;

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
            var effectiveParallelism = Math.Min(parallelism, 3);

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
                ".runtime",
                "tmp",
                $"playwright-worldfootball-session-{Environment.ProcessId}");

            Directory.CreateDirectory(sessionDir);

            var sharedContext = await playwright.Chromium.LaunchPersistentContextAsync(sessionDir, new BrowserTypeLaunchPersistentContextOptions
            {
                Channel = "chrome",
                Headless = false,
                SlowMo = 50,
                ChromiumSandbox = false,
                ViewportSize = new ViewportSize
                {
                    Width = 1600,
                    Height = 1000
                },
                ScreenSize = new ScreenSize
                {
                    Width = 1600,
                    Height = 1000
                },
                UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                Locale = "en-US",
                IgnoreHTTPSErrors = true,
                Args = new[]
                {
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-dev-shm-usage",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--start-maximized"
                }
            });
            PrepareWorldFootballContext(sharedContext);

            var throttler = new SemaphoreSlim(effectiveParallelism, effectiveParallelism);
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
                Message = $"Claimed {pendingList.Count} partidos. Parallelism={parallelism}. EffectiveBrowserParallelism={effectiveParallelism}.",
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

            var awayRowInserted = false;
            Exception? lastException = null;

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

            var debugFolder = EnsureDebugFolder();
            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");

            for (var attempt = 1; attempt <= 3; attempt++)
            {
                IPage? page = null;
                try
                {
                    if (attempt > 1)
                        Console.WriteLine($"REINTENTO {attempt}/3 MatchId {row.MatchId}");

                    page = await context.NewPageAsync();

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
                    lastException = ex;
                    if (attempt < 3)
                    {
                        Console.WriteLine($"REINTENTABLE timeout MatchId {row.MatchId}: {ex.Message}");
                        await Task.Delay(1500);
                        continue;
                    }
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    if (attempt < 3 && IsRetryableWorldFootballException(ex))
                    {
                        Console.WriteLine($"REINTENTABLE MatchId {row.MatchId}: {ex.Message}");
                        await DelayBeforeWorldFootballRetryAsync();
                        continue;
                    }
                }
                finally
                {
                    if (page != null)
                    {
                        try { await page.CloseAsync(); } catch { }
                    }
                }
            }

            if (lastException is TaskCanceledException canceled)
            {
                Console.WriteLine($"TASK CANCELADA MatchId {row.MatchId}: {canceled.Message}");

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
                    Detail = canceled.Message,
                    Status = "CANCELED",
                    Database = BuildDatabaseWriteResult(awayRowInserted, updatedRows)
                };
            }

            Console.WriteLine($"ERROR MatchId {row.MatchId}: {lastException}");

            var finalUpdatedRows = await MarkMatchAsErrorAsync(conn, row.MatchId, "ERROR");

            return new LineupProcessResult
            {
                Index = index,
                TotalBatch = totalBatch,
                LineupId = row.LineupId,
                MatchId = row.MatchId,
                Team = row.Team,
                Opponent = row.Opponent,
                Error = lastException?.Message,
                Detail = lastException?.ToString(),
                Status = "ERROR",
                Database = BuildDatabaseWriteResult(awayRowInserted, finalUpdatedRows)
            };
        }

        private static async Task<List<LineupRow>> ClaimPendingHomeLineupsAsync(SqlConnection conn, int take)
        {
            var rows = await conn.QueryAsync<LineupRow>(@"
                DECLARE @Claimed TABLE (LineupId INT PRIMARY KEY);

                INSERT INTO @Claimed (LineupId)
                SELECT TOP (@Take) LineupId
                FROM dbo.Lineups WITH (UPDLOCK, READPAST, ROWLOCK, INDEX(IX_Lineups_Status))
                WHERE IsHome = 1
                  AND ScrapingStatus = 'PENDING'
                ORDER BY
                    CASE
                        WHEN League LIKE '%2-bundesliga%' THEN 1
                        ELSE 0
                    END,
                    CASE
                        WHEN Team LIKE '%' + CHAR(10) + '%'
                          OR Team LIKE '%' + CHAR(13) + '%'
                          OR Opponent LIKE '%' + CHAR(10) + '%'
                          OR Opponent LIKE '%' + CHAR(13) + '%'
                          OR Team LIKE '% 2'
                          OR Opponent LIKE '% 2'
                        THEN 1
                        ELSE 0
                    END,
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
            var scheduleUrl = BuildWorldFootballAllMatchesUrl(row);
            var scheduleMatchUrl = await ResolveWorldFootballMatchUrlFromCompetitionScheduleAsync(page, row);

            if (!string.IsNullOrWhiteSpace(scheduleMatchUrl))
            {
                Console.WriteLine($"WORLDFOOTBALL SCHEDULE RESULT: {scheduleMatchUrl}");
                return scheduleMatchUrl;
            }

            if (!string.IsNullOrWhiteSpace(scheduleUrl))
            {
                var directReportUrlForKnownLeague = BuildDirectWorldFootballReportUrl(row);
                if (ShouldUseDirectReportFallback(row.League) && !string.IsNullOrWhiteSpace(directReportUrlForKnownLeague))
                {
                    Console.WriteLine($"WORLDFOOTBALL DIRECT REPORT FALLBACK: {directReportUrlForKnownLeague}");
                    return directReportUrlForKnownLeague;
                }

                Console.WriteLine("WORLDFOOTBALL: calendario disponible pero sin match confiable; se evita fallback externo para no caer en 404 o errores de buscador.");
                return null;
            }

            var directReportUrl = BuildDirectWorldFootballReportUrl(row);

            if (!string.IsNullOrWhiteSpace(directReportUrl))
            {
                Console.WriteLine($"WORLDFOOTBALL DIRECT REPORT URL: {directReportUrl}");
                return directReportUrl;
            }

            return null;
        }

        private static async Task<string?> ResolveWorldFootballMatchUrlFromCompetitionScheduleAsync(IPage page, LineupRow row)
        {
            var scheduleUrl = BuildWorldFootballAllMatchesUrl(row);
            if (string.IsNullOrWhiteSpace(scheduleUrl))
                return null;

            await WaitForWorldFootballCooldownAsync();
            Console.WriteLine($"WORLDFOOTBALL SCHEDULE URL: {scheduleUrl}");

            await page.GotoAsync(scheduleUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = 45000
            });

            await EnsureWorldFootballVerificationClearedAsync(page);
            await page.WaitForTimeoutAsync(1500);

            var candidates = await page.EvaluateAsync<ScheduleCandidateLink[]>(@"
                () => {
                    const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
                    const list = [];
                    const matches = Array.from(document.querySelectorAll('.match[data-match_id], div.match[data-match_id]'));

                    for (const match of matches) {
                        const href =
                            match.querySelector('.match-result a[href*=""/match-report/""]')?.href ||
                            match.querySelector('.match-more a[href*=""/match-report/""]')?.href ||
                            null;

                        if (!href) continue;

                        const home = clean(match.querySelector('.team-name-home')?.textContent || match.querySelector('.team-shortname-home')?.textContent);
                        const away = clean(match.querySelector('.team-name-away')?.textContent || match.querySelector('.team-shortname-away')?.textContent);
                        const isoDate = (match.getAttribute('data-datetime') || '').slice(0, 10);
                        const uiDate = isoDate ? isoDate.split('-').reverse().join('.') : '';
                        const visibleDate =
                            clean(match.querySelector('.match-date')?.textContent || '') ||
                            clean(match.querySelector('.date')?.textContent || '') ||
                            clean(match.querySelector('[class*=""date""]')?.textContent || '');
                        const rowText = clean([uiDate, visibleDate, home, away, clean(match.innerText)].filter(Boolean).join(' '));

                        list.push({
                            href,
                            linkText: clean(match.querySelector('.match-result a')?.textContent || ''),
                            rowText
                        });
                    }

                    return list;
                }
            ");

            var best = candidates
                .Where(x => !string.IsNullOrWhiteSpace(x.Href))
                .DistinctBy(x => x.Href)
                .Select(x => new
                {
                    x.Href,
                    Score = ScoreScheduleCandidate(x, row)
                })
                .Where(x => x.Score > 0)
                .OrderByDescending(x => x.Score)
                .Select(x => x.Href)
                .FirstOrDefault();

            if (!string.IsNullOrWhiteSpace(best))
                return best;

            await SaveWorldFootballSearchDebugArtifactsAsync(page, $"schedule_{row.League}_{row.Team}_{row.Opponent}_{row.MatchDate:yyyyMMdd}");
            return null;
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

                    var looksLikeRegularNotFoundPage =
                        !string.IsNullOrWhiteSpace(title) && title.Contains("404", StringComparison.OrdinalIgnoreCase)
                        && !string.IsNullOrWhiteSpace(text)
                        && text.Contains("couldn't find the page", StringComparison.OrdinalIgnoreCase);

                    if (looksLikeRegularNotFoundPage)
                        return false;

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

            TriggerWorldFootballCooldown("verificación de seguridad de WorldFootball");
            throw new Exception("WorldFootball mostró verificación de seguridad y se activó un cooldown automático.");
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

        private static async Task<bool> IsSearchEngineChallengePageAsync(IPage page)
        {
            try
            {
                var title = await page.TitleAsync();
                var text = await page.EvaluateAsync<string>("() => document.body ? document.body.innerText : ''");
                var html = await page.ContentAsync();

                return (!string.IsNullOrWhiteSpace(title) && (
                            title.Contains("One last step", StringComparison.OrdinalIgnoreCase)
                            || title.Contains("Just a moment", StringComparison.OrdinalIgnoreCase)
                            || title.Contains("Verify", StringComparison.OrdinalIgnoreCase)))
                       || (!string.IsNullOrWhiteSpace(text) && (
                            text.Contains("Please solve the challenge below to continue", StringComparison.OrdinalIgnoreCase)
                            || text.Contains("cf-turnstile-response", StringComparison.OrdinalIgnoreCase)
                            || text.Contains("security verification", StringComparison.OrdinalIgnoreCase)))
                       || (!string.IsNullOrWhiteSpace(html) && (
                            html.Contains("cf-turnstile-response", StringComparison.OrdinalIgnoreCase)
                            || html.Contains("challenge/verify", StringComparison.OrdinalIgnoreCase)
                            || html.Contains("turnstile", StringComparison.OrdinalIgnoreCase)));
            }
            catch
            {
                return false;
            }
        }

        private static int ScoreCandidateUrl(string url, LineupRow row)
        {
            if (string.IsNullOrWhiteSpace(url))
                return 0;

            var score = 0;
            var normalizedUrl = RemoveDiacritics(url).ToLowerInvariant();
            if (ContainsTeamReference(normalizedUrl, row.Team)) score += 25;
            if (ContainsTeamReference(normalizedUrl, row.Opponent)) score += 25;

            var competitionVariants = BuildCompetitionReferenceVariants(row.League);

            if (normalizedUrl.Contains("worldfootball.net")) score += 10;
            if (normalizedUrl.Contains("/match-report/")) score += 30;
            if (normalizedUrl.Contains("/lineup/")) score += 25;
            if (normalizedUrl.Contains("/matches-against/")) score += 12;
            if (normalizedUrl.Contains("/teams/")) score += 8;
            if (competitionVariants.Any(variant => normalizedUrl.Contains(variant, StringComparison.OrdinalIgnoreCase))) score += 12;

            return score;
        }

        private static List<string> BuildCompetitionReferenceVariants(string? league)
        {
            var variants = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var normalized = NormalizeCompetitionKey(league);

            if (!string.IsNullOrWhiteSpace(normalized))
            {
                variants.Add(normalized);
                variants.Add(normalized.Replace(" ", "-"));
            }

            var reportSlug = MapCompetitionToWorldFootballReportSlug(league);
            if (!string.IsNullOrWhiteSpace(reportSlug))
                variants.Add(reportSlug);

            var allMatchesSlug = MapCompetitionToWorldFootballAllMatchesSlug(league);
            if (!string.IsNullOrWhiteSpace(allMatchesSlug))
            {
                variants.Add(allMatchesSlug);

                var countryAgnosticSlug = Regex.Replace(allMatchesSlug, @"^[a-z]{3}-", "");
                variants.Add(countryAgnosticSlug);
            }

            return variants
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => NormalizeTeamKey(x).Replace(" ", "-"))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static int ScoreScheduleCandidate(ScheduleCandidateLink candidate, LineupRow row)
        {
            if (candidate == null || string.IsNullOrWhiteSpace(candidate.Href))
                return 0;

            var score = ScoreCandidateUrl(candidate.Href, row);
            var rowText = RemoveDiacritics(candidate.RowText ?? "").ToLowerInvariant();
            var linkText = RemoveDiacritics(candidate.LinkText ?? "").ToLowerInvariant();

            if (row.MatchDate.HasValue)
            {
                var date1 = row.MatchDate.Value.ToString("dd/MM/yyyy").ToLowerInvariant();
                var date2 = row.MatchDate.Value.ToString("d/M/yyyy").ToLowerInvariant();
                var date3 = row.MatchDate.Value.ToString("dd.MM.yyyy").ToLowerInvariant();
                var date4 = row.MatchDate.Value.ToString("d.M.yyyy").ToLowerInvariant();
                var dateIso = row.MatchDate.Value.ToString("yyyy-MM-dd").ToLowerInvariant();
                if (rowText.Contains(date1) || rowText.Contains(date2) || rowText.Contains(date3) || rowText.Contains(date4) || rowText.Contains(dateIso))
                    score += 40;
            }

            if (ContainsTeamReference(rowText, row.Team)) score += 35;
            if (ContainsTeamReference(rowText, row.Opponent)) score += 35;
            if (ContainsTeamReference(linkText, row.Team)) score += 10;
            if (ContainsTeamReference(linkText, row.Opponent)) score += 10;
            if ((ContainsTeamReference(rowText, row.Team) && !ContainsTeamReference(rowText, row.Opponent))
                || (!ContainsTeamReference(rowText, row.Team) && ContainsTeamReference(rowText, row.Opponent)))
                score -= 40;

            return score;
        }

        private static async Task<string?> ResolveLineupUrlFromMatchupPageAsync(IPage page, string matchupOrMatchReportUrl, LineupRow row)
        {
            // La pagina report suele contener el detalle suficiente y evita 404
            // frecuentes en variantes /lineup/ para algunas ligas y copas.
            if (matchupOrMatchReportUrl.Contains("/match-report/", StringComparison.OrdinalIgnoreCase))
            {
                return NormalizeWorldFootballReportUrl(matchupOrMatchReportUrl);
            }

            if (matchupOrMatchReportUrl.Contains("/report/", StringComparison.OrdinalIgnoreCase))
            {
                return NormalizeWorldFootballReportUrl(matchupOrMatchReportUrl);
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

            return string.IsNullOrWhiteSpace(lineupUrl) ? null : NormalizeWorldFootballReportUrl(lineupUrl);
        }

        private static string NormalizeWorldFootballReportUrl(string url)
        {
            if (string.IsNullOrWhiteSpace(url))
                return url;

            var clean = url.Split('?')[0].Split('#')[0];

            if (clean.Contains("/match-report/", StringComparison.OrdinalIgnoreCase))
                return clean.EndsWith("/") ? clean : clean + "/";

            clean = clean.Replace("/lineup/", "/", StringComparison.OrdinalIgnoreCase);

            if (clean.EndsWith("/head-to-head/", StringComparison.OrdinalIgnoreCase))
                return clean.Replace("/head-to-head/", "/", StringComparison.OrdinalIgnoreCase);

            if (clean.EndsWith("/standings/", StringComparison.OrdinalIgnoreCase))
                return clean.Replace("/standings/", "/", StringComparison.OrdinalIgnoreCase);

            if (clean.Contains("/match-report/", StringComparison.OrdinalIgnoreCase))
            {
                return clean.EndsWith("/") ? clean : clean + "/";
            }

            if (clean.Contains("/report/", StringComparison.OrdinalIgnoreCase))
            {
                return clean.EndsWith("/") ? clean : clean + "/";
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

            var title = await page.TitleAsync();
            var html = await page.ContentAsync();
            var visibleText = await page.EvaluateAsync<string>("() => document.body ? document.body.innerText : ''");

            if (LooksLikeWorldFootballNotFoundPage(title, visibleText))
                throw new Exception($"WorldFootball devolvio 404 para la URL de lineup/report: {lineupUrl}");

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
                ".runtime",
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

        private static void PrepareWorldFootballContext(IBrowserContext context)
        {
            context.SetDefaultTimeout(45000);
            context.SetDefaultNavigationTimeout(45000);
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

        private static void TriggerWorldFootballCooldown(string reason)
        {
            var until = DateTimeOffset.UtcNow.Add(WorldFootballCooldownWindow);

            lock (WorldFootballCooldownLock)
            {
                if (!_worldFootballCooldownUntilUtc.HasValue || _worldFootballCooldownUntilUtc.Value < until)
                    _worldFootballCooldownUntilUtc = until;
            }

            Console.WriteLine($"WORLDFOOTBALL COOLDOWN ACTIVADO hasta {until:O}. Motivo: {reason}");
        }

        private static TimeSpan GetRemainingWorldFootballCooldown()
        {
            lock (WorldFootballCooldownLock)
            {
                if (!_worldFootballCooldownUntilUtc.HasValue)
                    return TimeSpan.Zero;

                var remaining = _worldFootballCooldownUntilUtc.Value - DateTimeOffset.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    _worldFootballCooldownUntilUtc = null;
                    return TimeSpan.Zero;
                }

                return remaining;
            }
        }

        private static async Task WaitForWorldFootballCooldownAsync()
        {
            var remaining = GetRemainingWorldFootballCooldown();
            if (remaining <= TimeSpan.Zero)
                return;

            Console.WriteLine($"WORLDFOOTBALL COOLDOWN EN CURSO: esperando {remaining.TotalSeconds:F0}s antes de volver a intentar.");
            await Task.Delay(remaining + TimeSpan.FromSeconds(1));
        }

        private static async Task DelayBeforeWorldFootballRetryAsync()
        {
            var remaining = GetRemainingWorldFootballCooldown();
            if (remaining > TimeSpan.Zero)
            {
                await WaitForWorldFootballCooldownAsync();
                return;
            }

            await Task.Delay(2000);
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

        private static bool ShouldUseDirectReportFallback(string? league)
        {
            var key = NormalizeCompetitionKey(league);

            return key is "bundesliga"
                or "2 bundesliga"
                or "dfb pokal"
                or "laliga"
                or "segunda division"
                or "copa del rey";
        }

        private static string? BuildWorldFootballAllMatchesUrl(LineupRow row)
        {
            var seasonSlug = BuildSeasonSlug(row);
            var key = NormalizeCompetitionKey(row.League);

            if (string.IsNullOrWhiteSpace(seasonSlug))
                return null;

            if (key == "bundesliga")
                return $"https://www.worldfootball.net/all_matches/bundesliga/bundesliga-{seasonSlug}/";

            var competitionSlug = MapCompetitionToWorldFootballAllMatchesSlug(row.League);
            if (string.IsNullOrWhiteSpace(competitionSlug))
                return null;

            return $"https://www.worldfootball.net/all_matches/{competitionSlug}-{seasonSlug}/";
        }

        private static string? MapCompetitionToWorldFootballReportSlug(string? league)
        {
            var key = NormalizeCompetitionKey(league);

            return key switch
            {
                "bundesliga" => "bundesliga",
                "2 bundesliga" => "2-bundesliga",
                "dfb pokal" => "dfb-pokal",
                "premier league" => "premier-league",
                "championship" => "championship",
                "fa cup" => "fa-cup",
                "league cup" => "league-cup",
                "serie a" => "serie-a",
                "serie b" => "serie-b",
                "coppa italia" => "coppa-italia",
                "ligue 1" => "ligue-1",
                "ligue 2" => "ligue-2",
                "coupe de france" => "coupe-de-france",
                "laliga" => "primera-division",
                "segunda division" => "segunda-division",
                "copa del rey" => "copa-del-rey",
                _ => null
            };
        }

        private static string? MapCompetitionToWorldFootballAllMatchesSlug(string? league)
        {
            var key = NormalizeCompetitionKey(league);

            return key switch
            {
                "bundesliga" => "bundesliga",
                "2 bundesliga" => "2-bundesliga",
                "dfb pokal" => "dfb-pokal",
                "premier league" => "eng-premier-league",
                "championship" => "eng-championship",
                "fa cup" => "eng-fa-cup",
                "league cup" => "eng-league-cup",
                "serie a" => "ita-serie-a",
                "serie b" => "ita-serie-b",
                "coppa italia" => "ita-coppa-italia",
                "ligue 1" => "fra-ligue-1",
                "ligue 2" => "fra-ligue-2",
                "coupe de france" => "fra-coupe-de-france",
                "laliga" => "esp-primera-division",
                "segunda division" => "esp-segunda-division",
                "copa del rey" => "esp-copa-del-rey",
                _ => null
            };
        }

        private static string NormalizeCompetitionKey(string? league)
        {
            var key = NormalizeTeamKey(league ?? "");

            return key switch
            {
                "1 bundesliga" => "bundesliga",
                "germany bundesliga" => "bundesliga",
                "german bundesliga" => "bundesliga",
                "bundesliga germany" => "bundesliga",
                "2 bundesliga germany" => "2 bundesliga",
                "germany 2 bundesliga" => "2 bundesliga",
                "2 bundesliga men" => "2 bundesliga",
                "2 bundesliga german" => "2 bundesliga",
                "2 bundesliga germany men" => "2 bundesliga",
                "dfb pokal germany" => "dfb pokal",
                "germany dfb pokal" => "dfb pokal",
                "german cup" => "dfb pokal",
                "england premier league" => "premier league",
                "english premier league" => "premier league",
                "england championship" => "championship",
                "efl championship" => "championship",
                "english championship" => "championship",
                "england fa cup" => "fa cup",
                "english fa cup" => "fa cup",
                "efl cup" => "league cup",
                "carabao cup" => "league cup",
                "england league cup" => "league cup",
                "english league cup" => "league cup",
                "italy serie a" => "serie a",
                "serie a italy" => "serie a",
                "italy serie b" => "serie b",
                "serie b italy" => "serie b",
                "coppa italia italy" => "coppa italia",
                "france ligue 1" => "ligue 1",
                "ligue 1 france" => "ligue 1",
                "france ligue 2" => "ligue 2",
                "ligue 2 france" => "ligue 2",
                "france coupe de france" => "coupe de france",
                "coupe de france france" => "coupe de france",
                "la liga" => "laliga",
                "laliga santander" => "laliga",
                "laliga ea sports" => "laliga",
                "spain laliga" => "laliga",
                "spain la liga" => "laliga",
                "spanish la liga" => "laliga",
                "primera division" => "laliga",
                "spain primera division" => "laliga",
                "segunda division" => "segunda division",
                "laliga 2" => "segunda division",
                "laliga2" => "segunda division",
                "segunda division spain" => "segunda division",
                "spain segunda division" => "segunda division",
                "la liga 2" => "segunda division",
                "copa del rey spain" => "copa del rey",
                "spain copa del rey" => "copa del rey",
                "spanish cup" => "copa del rey",
                _ => key
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
                "alaves" => "cd-alaves",
                "arminia bielefeld" => "arminia-bielefeld",
                "angers" => "angers-sco",
                "as roma" => "as-roma",
                "athletic bilbao" => "athletic-bilbao",
                "athletic club" => "athletic-bilbao",
                "atletico madrid" => "atletico-madrid",
                "atalanta" => "atalanta-bc",
                "aue" => "erzgebirge-aue",
                "erzgebirge aue" => "erzgebirge-aue",
                "auxerre" => "aj-auxerre",
                "augsburg" => "fc-augsburg",
                "avellino" => "us-avellino-1912",
                "bari" => "ssc-bari",
                "b monchengladbach" => "bor-moenchengladbach",
                "bor monchengladbach" => "bor-moenchengladbach",
                "monchengladbach" => "bor-moenchengladbach",
                "bayer leverkusen" => "bayer-leverkusen",
                "bayern munich" => "bayern-munich",
                "barcelona" => "fc-barcelona",
                "bologna" => "bologna-fc-1909",
                "bochum" => "vfl-bochum",
                "braunschweig" => "eintracht-braunschweig",
                "bremen" => "bremer-sv",
                "brescia" => "brescia-calcio",
                "brest" => "stade-brestois-29",
                "cadiz" => "cadiz-cf",
                "cottbus" => "energie-cottbus",
                "carrarese" => "carrarese-calcio",
                "catanzaro" => "us-catanzaro",
                "celta vigo" => "rc-celta",
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
                "elche" => "elche-cf",
                "espanyol" => "espanyol-barcelona",
                "eintracht frankfurt" => "eintracht-frankfurt",
                "elversberg" => "sv-07-elversberg",
                "empoli" => "empoli-fc",
                "fc koln" => "1-fc-koeln",
                "fc cologne" => "1-fc-koeln",
                "fiorentina" => "acf-fiorentina",
                "freiburg" => "sc-freiburg",
                "frosinone" => "frosinone-calcio",
                "genoa" => "genoa-cfc",
                "getafe" => "getafe-cf",
                "greifswald" => "greifswalder-fc",
                "girona" => "girona-fc",
                "granada" => "granada-cf",
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
                "las palmas" => "ud-las-palmas",
                "lazio" => "lazio-rom",
                "le havre" => "le-havre-ac",
                "lecce" => "us-lecce",
                "lens" => "rc-lens",
                "levante" => "levante-ud",
                "lille" => "losc-lille",
                "lotte" => "sportfreunde-lotte",
                "lyon" => "olympique-lyonnais",
                "mainz" => "1-fsv-mainz-05",
                "magdeburg" => "1-fc-magdeburg",
                "mantova" => "mantova-1911",
                "marseille" => "olympique-marseille",
                "mallorca" => "rcd-mallorca",
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
                "osasuna" => "ca-osasuna",
                "paderborn" => "sc-paderborn-07",
                "palermo" => "palermo-fc",
                "parma" => "parma-calcio-1913",
                "phonix lubeck" => "1-fc-phoenix-luebeck",
                "phoenix lubeck" => "1-fc-phoenix-luebeck",
                "pisa" => "pisa-sc",
                "preussen munster" => "sc-preussen-muenster",
                "psg" => "paris-saint-germain",
                "rayo vallecano" => "rayo-vallecano",
                "rb leipzig" => "rb-leipzig",
                "real betis" => "real-betis",
                "betis" => "real-betis",
                "real madrid" => "real-madrid",
                "real sociedad" => "real-sociedad",
                "real valladolid" => "real-valladolid",
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
                "sevilla" => "sevilla-fc",
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
                "valencia" => "valencia-cf",
                "venezia" => "venezia-fc",
                "verona" => "hellas-verona",
                "vfl osnabruck" => "vfl-osnabrueck",
                "viktoria berlin" => "fc-viktoria-1889-berlin",
                "villingen" => "fc-08-villingen",
                "villarreal" => "villarreal-cf",
                "wehen" => "sv-wehen-wiesbaden",
                "werder bremen" => "werder-bremen",
                "wolfsburg" => "vfl-wolfsburg",
                "wurzburger kickers" => "wuerzburger-kickers",
                _ => null
            };
        }

        private static string NormalizeTeamKey(string value)
        {
            var sanitized = SanitizeTeamLabel(value);

            var normalized = RemoveDiacritics(sanitized)
                .ToLowerInvariant()
                .Replace(".", " ")
                .Replace("-", " ");

            normalized = Regex.Replace(normalized, @"\s+", " ").Trim();
            return normalized;
        }

        private static string SanitizeTeamLabel(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return value;

            var hadControlChars = value.IndexOfAny(new[] { '\r', '\n', '\t' }) >= 0;

            var sanitized = value
                .Replace('\r', ' ')
                .Replace('\n', ' ')
                .Replace('\t', ' ');

            sanitized = Regex.Replace(sanitized, @"\s+", " ").Trim();

            if (hadControlChars)
                sanitized = Regex.Replace(sanitized, @"\s+\d+\s*$", "");

            return sanitized.Trim();
        }

        private static bool ContainsTeamReference(string? text, string? teamName)
        {
            if (string.IsNullOrWhiteSpace(text) || string.IsNullOrWhiteSpace(teamName))
                return false;

            var normalizedText = NormalizeTeamKey(text);
            var variants = BuildTeamReferenceVariants(teamName);

            foreach (var variant in variants)
            {
                if (string.IsNullOrWhiteSpace(variant))
                    continue;

                if (normalizedText.Contains(variant, StringComparison.OrdinalIgnoreCase))
                    return true;

                var tokens = TokenizeTeamName(variant);
                if (tokens.Count >= 2 && tokens.Count(t => normalizedText.Contains(t, StringComparison.OrdinalIgnoreCase)) >= Math.Min(2, tokens.Count))
                    return true;
            }

            return false;
        }

        private static List<string> BuildTeamReferenceVariants(string? teamName)
        {
            var variants = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var normalized = NormalizeTeamKey(teamName ?? "");

            if (!string.IsNullOrWhiteSpace(normalized))
                variants.Add(normalized);

            var slug = MapTeamToWorldFootballReportSlug(teamName);
            if (!string.IsNullOrWhiteSpace(slug))
            {
                variants.Add(slug.Replace("-", " "));
                variants.Add(NormalizeTeamKey(slug.Replace("-", " ")));
            }

            foreach (var alias in ExpandCommonTeamAliases(normalized))
                variants.Add(alias);

            return variants
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => NormalizeTeamKey(x))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static IEnumerable<string> ExpandCommonTeamAliases(string normalized)
        {
            if (string.IsNullOrWhiteSpace(normalized))
                yield break;

            yield return Regex.Replace(normalized, @"\b(fc|cf|sv|as|ac|ssc|us|vfl|vfb|borussia|eintracht|spvgg|tsg|sc)\b", " ").Trim();

            if (normalized.Contains("koln")) yield return normalized.Replace("koln", "koeln");
            if (normalized.Contains("koeln")) yield return normalized.Replace("koeln", "koln");
            if (normalized.Contains("nurnberg")) yield return normalized.Replace("nurnberg", "nuernberg");
            if (normalized.Contains("nuernberg")) yield return normalized.Replace("nuernberg", "nurnberg");
            if (normalized.Contains("munich")) yield return normalized.Replace("munich", "muenchen");
            if (normalized.Contains("muenchen")) yield return normalized.Replace("muenchen", "munich");
            if (normalized.Contains("munich")) yield return normalized.Replace("munich", "munchen");
            if (normalized.Contains("munchen")) yield return normalized.Replace("munchen", "munich");
            if (normalized.Contains("muenchen")) yield return normalized.Replace("muenchen", "munchen");
            if (normalized.Contains("munchen")) yield return normalized.Replace("munchen", "muenchen");
            if (normalized.Contains("hertha berlin")) yield return "hertha bsc";
            if (normalized.Contains("hamburger sv")) yield return "hamburg";
            if (normalized == "aue") yield return "erzgebirge aue";
            if (normalized.Contains("erzgebirge aue")) yield return "aue";
            if (normalized.Contains("fc koln")) yield return "1 fc koln";
            if (normalized.Contains("fc koln")) yield return "1 fc koeln";
            if (normalized.Contains("braunschweig")) yield return "eintracht braunschweig";
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

        private static bool IsRetryableWorldFootballException(Exception ex)
        {
            var message = ex.ToString();

            return ex is PlaywrightException
                || message.Contains("Cloudflare", StringComparison.OrdinalIgnoreCase)
                || message.Contains("security verification", StringComparison.OrdinalIgnoreCase)
                || message.Contains("ERR_CONNECTION_CLOSED", StringComparison.OrdinalIgnoreCase)
                || message.Contains("ERR_CONNECTION_RESET", StringComparison.OrdinalIgnoreCase)
                || message.Contains("ERR_CONNECTION_REFUSED", StringComparison.OrdinalIgnoreCase)
                || message.Contains("ERR_TIMED_OUT", StringComparison.OrdinalIgnoreCase)
                || message.Contains("ERR_ABORTED", StringComparison.OrdinalIgnoreCase)
                || message.Contains("Failed to open a new tab", StringComparison.OrdinalIgnoreCase)
                || message.Contains("Target.createTarget", StringComparison.OrdinalIgnoreCase)
                || message.Contains("Target page, context or browser has been closed", StringComparison.OrdinalIgnoreCase)
                || message.Contains("No se pudo resolver la URL de matchup", StringComparison.OrdinalIgnoreCase)
                || message.Contains("WorldFootball devolvio 404", StringComparison.OrdinalIgnoreCase);
        }

        private static bool LooksLikeWorldFootballNotFoundPage(string? title, string? text)
        {
            if (string.IsNullOrWhiteSpace(title) && string.IsNullOrWhiteSpace(text))
                return false;

            return (!string.IsNullOrWhiteSpace(title) && title.Contains("404", StringComparison.OrdinalIgnoreCase))
                || (!string.IsNullOrWhiteSpace(text) && (
                    text.Contains("couldn't find the page", StringComparison.OrdinalIgnoreCase)
                    || text.Contains("nicht gefunden", StringComparison.OrdinalIgnoreCase)
                ));
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

                if (ContainsTeamReference(key, normalizedTeam) || ContainsTeamReference(normalizedTeam, key))
                    return kv.Value;
            }

            // Token score
            var best = map
                .Select(kv => new
                {
                    kv.Value,
                    Score = BuildTeamReferenceVariants(normalizedTeam)
                        .Max(variant => TokenizeTeamName(variant)
                            .Count(t => RemoveDiacritics(kv.Key).ToLowerInvariant().Contains(t)))
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

        private class ScheduleCandidateLink
        {
            public string? Href { get; set; }
            public string? LinkText { get; set; }
            public string? RowText { get; set; }
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
