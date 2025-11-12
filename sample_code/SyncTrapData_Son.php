<?php

namespace App\Console\Commands\Traps;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use App\Jobs\InsertContourLocationsChunk;
use Carbon\Carbon;

class SyncContourData extends Command
{
    protected $signature = 'traps:sync-contour '
        . '{--region-name= : Contour region name to sync into} '
        . '{--from= : Start date (YYYY-MM-DD) for counts} '
        . '{--to= : End date (YYYY-MM-DD, exclusive) for counts} '
        . '{--chunk=500 : Insert chunk size} '
        . '{--dry-run : Only print actions, no DB writes} '
        . '{--model-id= : Optional model id to scope counts uniqueness check}';

    protected $description = 'Sync Scout Labs traps into contour_locations and prepare contour_trap_counts rows.';

    public function handle(): int
    {
        $regionName = $this->option('region-name') ?: env('CONTOUR_REGION_NAME');
        if (!$regionName) {
            $this->error('Region name is required. Pass --region-name or set CONTOUR_REGION_NAME in .env');
            return self::INVALID;
        }

        $modelsSchema = env('DB_MODELS_SCHEMA', 'service_models');
        $weatherSchema = env('DB_WEATHER_SCHEMA', 'service_weather');

        // Fetch contour region id dynamically
        $regionId = DB::table($modelsSchema . '.contour_regions')
            ->whereRaw("CONVERT(name USING utf8mb4) COLLATE utf8mb4_unicode_ci = ?", [$regionName])
            ->value('id');

        if (!$regionId) {
            $this->error("Contour region not found for name: {$regionName}");
            return self::FAILURE;
        }

        $surveyYear = (int) now()->year;
        $chunkSize = (int) $this->option('chunk');
        $dryRun = (bool) $this->option('dry-run');

        $this->info('Scanning for new locations from Scout Labs traps...');
        $existingNames = DB::table($modelsSchema . '.contour_locations')
            ->where('contour_region_id', $regionId)
            ->where('survey_year', $surveyYear)
            ->pluck('name')
            ->map(fn ($n) => mb_strtolower($n))
            ->toArray();
        $existingSet = array_flip($existingNames);

        $traps = DB::table($weatherSchema . '.scout_labs_traps')
            ->whereNotNull('lat')
            ->whereNotNull('lng')
            ->select(['name', 'lat', 'lng'])
            ->get();

        $now = now();
        $newLocations = [];
        foreach ($traps as $t) {
            $key = mb_strtolower($t->name ?? '');
            if ($key === '' || isset($existingSet[$key])) {
                continue;
            }
            $newLocations[] = [
                'name' => $t->name,
                'lat' => $t->lat,
                'lng' => $t->lng,
                'survey_year' => $surveyYear,
                'contour_region_id' => $regionId,
                'created_at' => $now,
                'created_by' => 1,
            ];
        }

        $this->info("Found " . count($newLocations) . " new location(s).");

        if (count($newLocations) > 0) {
            if ($dryRun) {
                $this->line('Dry-run: not inserting locations. Showing first 5 rows:');
                $this->line(json_encode(array_slice($newLocations, 0, 5), JSON_PRETTY_PRINT));
            } else {
                // Chunk and dispatch to background job for insertion
                $chunks = array_chunk($newLocations, max(1, $chunkSize));
                foreach ($chunks as $i => $chunk) {
                    InsertContourLocationsChunk::dispatch($chunk, $modelsSchema);
                }
                $this->info('Dispatched ' . count($chunks) . ' job(s) to insert new locations.');
            }
        }

        // 2) Pull trap counts preparation
        $from = $this->option('from') ? Carbon::createFromFormat('Y-m-d', $this->option('from')) : Carbon::create(now()->year, 1, 1);
        $to = $this->option('to') ? Carbon::createFromFormat('Y-m-d', $this->option('to')) : now()->copy()->addDay();
        $modelId = $this->option('model-id');

        if ($from->gte($to)) {
            $this->error('Invalid date range: --from must be earlier than --to');
            return self::INVALID;
        }

        $this->info("Preparing trap counts from {$from->toDateString()} to {$to->toDateString()} (exclusive)");

        // Build base query mirroring console.sql, excluding already-imported rows when model-id provided
        $countsQuery = DB::table($weatherSchema . '.scout_labs_records as wr')
            ->join($weatherSchema . '.scout_labs_traps as wt', DB::raw('CAST(wt.trap_id AS CHAR(36))'), '=', DB::raw('CAST(wr.trap_id AS CHAR(36))'))
            ->join($modelsSchema . '.contour_locations as ml', function ($join) use ($regionId) {
                $join->on(
                    DB::raw("CONVERT(ml.name USING utf8mb4) COLLATE utf8mb4_unicode_ci"),
                    '=',
                    DB::raw("CONVERT(wt.name USING utf8mb4) COLLATE utf8mb4_unicode_ci")
                )
                    ->where('ml.contour_region_id', '=', $regionId)
                    ->whereRaw('ml.survey_year = YEAR(wr.recorded_at)');
            })
            ->where('wr.recorded_at', '>=', $from)
            ->where('wr.recorded_at', '<', $to)
            ->selectRaw('ml.id as location_id, DATE(wr.recorded_at) as survey_date, COALESCE(wr.detection_count,0) as trap_count');

        if ($modelId !== null && $modelId !== '') {
            $countsQuery->whereNotExists(function ($q) use ($modelsSchema, $modelId) {
                $q->select(DB::raw(1))
                    ->from($modelsSchema . '.contour_trap_counts as c')
                    ->whereColumn('c.location_id', 'ml.id')
                    ->whereColumn('c.survey_date', DB::raw('DATE(wr.recorded_at)'))
                    ->where('c.model_id', $modelId);
            });
        }

        $rows = $countsQuery->orderBy('survey_date', 'desc')->get();
        $this->info('Prepared ' . $rows->count() . ' trap count row(s) for insertion.');

        // Save a preview JSON to storage for inspection
        try {
            $preview = $rows->take(100)->map(fn ($r) => [
                'model_id' => $modelId,
                'location_id' => $r->location_id,
                'survey_date' => $r->survey_date,
                'trap_count' => $r->trap_count,
                'contour_id' => null,
            ]);
            Storage::disk('local')->put('contour_trap_counts_preview.json', json_encode($preview, JSON_PRETTY_PRINT));
            $this->info('Wrote preview to storage/app/contour_trap_counts_preview.json');
        } catch (\Throwable $e) {
            Log::warning('Failed to write preview file', ['error' => $e->getMessage()]);
        }

        $this->line('Done.');
        return self::SUCCESS;
    }
}
