<?php

namespace App\Console\Commands;

use App\Jobs\SyncContourDataJob;
use App\ORM\ModelCard;
use App\ORM\Potatoes\ContourLocation;
use App\ORM\Potatoes\ContourRegion;
use App\ORM\ScoutLabs\ScoutLabsRecord;
use App\ORM\ScoutLabs\ScoutLabsTrap;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Sync data from Scout Labs tables to Contour tables.
 *
 * Data Sources:
 * - Source Tables: scout_labs_traps, scout_labs_records
 * - Destination Tables: contour_locations, contour_trap_counts
 *
 * This command syncs all data from Scout Labs tables to Contour tables
 * without any date constraints or filtering. Each record is mapped directly
 * to a trap_count entry without weekly grouping.
 *
 * Usage:
 *   php artisan sync:scout-labs-to-contour --region-name="Scout Labs" --chunk=500 --sync
 */
class SyncScoutLabsToContour extends Command
{
    protected $signature = 'sync:scout-labs-to-contour
                            {--region-name= : Region name (e.g., "Scout Labs")}
                            {--chunk=500 : Number of records to process per chunk}
                            {--sync : Run jobs synchronously instead of queuing them}';

    protected $description = 'Sync data from Scout Labs tables (scout_labs_traps, scout_labs_records) to Contour tables (contour_locations, contour_trap_counts)';

    private int $chunkSize;

    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle(): void
    {
        $this->chunkSize = (int) $this->option('chunk');

        Log::info('Starting Scout Labs to Contour sync...');
        $this->info('Starting Scout Labs to Contour sync...');

        try {
            // Step 1: Fetch Contour Region ID
            $regionName = $this->option('region-name') ?: env('CONTOUR_REGION_NAME', 'Scout Labs');
            $regionId = $this->fetchContourRegionId($regionName);
            if (!$regionId) {
                $this->error("Region '{$regionName}' not found in contour_regions table.");
                Log::error("Region '{$regionName}' not found in contour_regions table.");
                return;
            }

            $this->info("Found region ID: {$regionId} ({$regionName})");

            // Step 2: Sync locations from scout_labs_traps to contour_locations
            $this->syncLocations($regionId);

            // If using async queue, wait for locations to be inserted before syncing trap counts
            if (!$this->option('sync') && config('queue.default') !== 'sync') {
                $this->info('Waiting for location inserts to complete...');
                sleep(2); // Give jobs a moment to process
                $this->info('Rebuilding location map to include newly inserted locations...');
            }

            // Step 3: Sync trap counts from scout_labs_records to contour_trap_counts
            $this->syncTrapCounts($regionId);

            Log::info('Scout Labs to Contour sync completed successfully.');
            $this->info('Scout Labs to Contour sync completed successfully.');
        } catch (\Throwable $e) {
            Log::error("Error during Scout Labs to Contour sync: {$e->getMessage()}", [
                'exception' => $e,
            ]);
            $this->error("Error: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Fetch the Contour region ID by region name.
     *
     * @param string $regionName The name of the region to find
     * @return int|null The region ID, or null if not found
     */
    private function fetchContourRegionId(string $regionName): ?int
    {
        $region = ContourRegion::where('name', $regionName)
            ->orWhere('name', 'LIKE', "%{$regionName}%")
            ->first();

        if (!$region) {
            $region = DB::table('contour_regions')
                ->whereRaw('LOWER(name) LIKE ?', ['%' . strtolower($regionName) . '%'])
                ->first();
        }

        return $region ? (is_object($region) ? $region->id : $region['id']) : null;
    }

    /**
     * Sync locations from scout_labs_traps to contour_locations.
     *
     * @param int $regionId The Contour region ID
     * @return void
     */
    private function syncLocations(int $regionId): void
    {
        $this->info('Syncing locations from scout_labs_traps to contour_locations...');
        Log::info('Syncing locations from scout_labs_traps to contour_locations...');

        // Get all existing locations
        $existingLocations = ContourLocation::select('name', 'lat', 'lng', 'survey_year')
            ->get()
            ->map(function ($location) {
                return $this->getLocationKey($location->name, $location->lat, $location->lng, $location->survey_year);
            })
            ->toArray();

        // Get all Scout Labs traps
        $scoutTraps = ScoutLabsTrap::all();
        $this->info("Found " . $scoutTraps->count() . " Scout Labs traps to process.");

        // Identify new locations
        $newLocations = [];
        foreach ($scoutTraps as $trap) {
            $surveyYear = date('Y');
            if (isset($trap->survey_year)) {
                $surveyYear = $trap->survey_year;
            } elseif (isset($trap->created_at)) {
                $surveyYear = $trap->created_at->year;
            }

            $locationName = $trap->smapp_id ?? $trap->name;
            $key = $this->getLocationKey($locationName, $trap->lat, $trap->lng, $surveyYear);

            if (!in_array($key, $existingLocations)) {
                $newLocations[] = [
                    'name' => $locationName,
                    'lat' => $trap->lat,
                    'lng' => $trap->lng,
                    'survey_year' => $surveyYear,
                ];
            }
        }

        if (empty($newLocations)) {
            $this->info('No new locations to sync.');
            return;
        }

        $this->info("Found " . count($newLocations) . " new locations to sync.");

        // Chunk and dispatch jobs
        $chunks = array_chunk($newLocations, $this->chunkSize);
        foreach ($chunks as $chunk) {
            if ($this->option('sync')) {
                (new SyncContourDataJob('locations', $chunk, $regionId))->handle();
            } else {
                SyncContourDataJob::dispatch('locations', $chunk, $regionId);
            }
        }

        $this->info("Dispatched " . count($chunks) . " job(s) for location sync.");
    }

    /**
     * Sync trap counts from scout_labs_records to contour_trap_counts.
     *
     * Maps each record directly to a trap_count entry without grouping.
     *
     * @param int $regionId The Contour region ID
     * @return void
     */
    private function syncTrapCounts(int $regionId): void
    {
        $this->info('Syncing trap counts from scout_labs_records to contour_trap_counts...');
        Log::info('Syncing trap counts from scout_labs_records to contour_trap_counts...');

        // Get all Scout Labs records
        $scoutRecords = ScoutLabsRecord::with('trap')->get();
        $this->info("Found " . $scoutRecords->count() . " Scout Labs records to process.");

        // Build location map
        $locationMap = $this->buildLocationMap();
        $this->info("Built location map with " . count($locationMap) . " locations.");

        // Process each record directly
        $trapCounts = [];
        $stats = [
            'no_trap' => 0,
            'no_survey_date' => 0,
            'no_model_id' => 0,
            'no_trap_count' => 0,
            'invalid_date_format' => 0,
            'no_matching_location' => 0,
            'already_exists' => 0,
            'added' => 0,
        ];

        foreach ($scoutRecords as $record) {
            if (!$record->trap) {
                $stats['no_trap']++;
                continue;
            }

            if (!$record->recorded_at) {
                $stats['no_survey_date']++;
                continue;
            }

            // Map pest_name to model_id
            $modelId = $this->getModelIdFromPestName($record->pest_name);
            if (!$modelId) {
                $stats['no_model_id']++;
                continue;
            }

            // Use detection_count as trap_count
            $trapCount = $record->detection_count ?? null;
            if ($trapCount === null) {
                $stats['no_trap_count']++;
                continue;
            }

            // Find matching location
            $surveyYear = date('Y');
            if (isset($record->trap->survey_year)) {
                $surveyYear = $record->trap->survey_year;
            } elseif (isset($record->trap->created_at)) {
                $surveyYear = $record->trap->created_at->year;
            }

            // Use the same name logic as when creating locations (smapp_id ?? name)
            $locationName = $record->trap->smapp_id ?? $record->trap->name;
            $locationKey = $this->getLocationKey(
                $locationName,
                $record->trap->lat,
                $record->trap->lng,
                $surveyYear
            );

            if (!isset($locationMap[$locationKey])) {
                $stats['no_matching_location']++;
                continue;
            }

            $locationId = $locationMap[$locationKey];

            // Parse recorded_at date - use the actual date, not week start
            $recordDate = null;
            if ($record->recorded_at instanceof \DateTime || $record->recorded_at instanceof Carbon) {
                $recordDate = $record->recorded_at instanceof Carbon
                    ? $record->recorded_at
                    : Carbon::instance($record->recorded_at);
            } elseif (is_string($record->recorded_at)) {
                $recordDate = Carbon::parse($record->recorded_at);
            }

            if (!$recordDate) {
                $stats['invalid_date_format']++;
                continue;
            }

            // Use the actual recorded date as survey_date
            $surveyDate = $recordDate->format('Y-m-d');

            // Check if this record already exists
            $exists = DB::table('contour_trap_counts')
                ->where('location_id', $locationId)
                ->where('model_id', $modelId)
                ->where('survey_date', $surveyDate)
                ->exists();

            if ($exists) {
                $stats['already_exists']++;
            } else {
                $stats['added']++;
                $trapCounts[] = [
                    'location_id' => $locationId,
                    'model_id' => $modelId,
                    'survey_date' => $surveyDate,
                    'trap_count' => $trapCount,
                ];
            }
        }

        // Display statistics
        $this->info("Processing Statistics:");
        $this->info("  - Records with no trap: {$stats['no_trap']}");
        $this->info("  - Records with no survey_date: {$stats['no_survey_date']}");
        $this->info("  - Records with no model_id: {$stats['no_model_id']}");
        $this->info("  - Records with no trap_count: {$stats['no_trap_count']}");
        $this->info("  - Records with invalid date format: {$stats['invalid_date_format']}");
        $this->info("  - Records with no matching location: {$stats['no_matching_location']}");
        $this->info("  - Records that already exist: {$stats['already_exists']}");
        $this->info("  - Records to insert: {$stats['added']}");

        if (empty($trapCounts)) {
            $this->warn('No new trap counts to sync.');
            return;
        }

        $this->info("Found " . count($trapCounts) . " new trap counts to sync.");

        // Chunk and dispatch jobs
        $chunks = array_chunk($trapCounts, $this->chunkSize);
        foreach ($chunks as $chunk) {
            if ($this->option('sync')) {
                (new SyncContourDataJob('trap_counts', $chunk))->handle();
            } else {
                SyncContourDataJob::dispatch('trap_counts', $chunk);
            }
        }

        $this->info("Dispatched " . count($chunks) . " job(s) for trap count sync.");
    }

    /**
     * Build a map of location keys to location IDs.
     *
     * @return array
     */
    private function buildLocationMap(): array
    {
        $locations = ContourLocation::all();
        $map = [];

        foreach ($locations as $location) {
            $key = $this->getLocationKey($location->name, $location->lat, $location->lng, $location->survey_year);
            $map[$key] = $location->id;
        }

        return $map;
    }

    /**
     * Generate a unique location key.
     *
     * @param string $name
     * @param float $lat
     * @param float $lng
     * @param int $surveyYear
     * @return string
     */
    private function getLocationKey(string $name, float $lat, float $lng, int $surveyYear): string
    {
        return sprintf(
            '%s|%.6f|%.6f|%d',
            $name,
            round($lat, 6),
            round($lng, 6),
            $surveyYear
        );
    }

    /**
     * Map pest name to model ID.
     *
     * @param string|null $pestName
     * @return int|null
     */
    private function getModelIdFromPestName(?string $pestName): ?int
    {
        if (!$pestName) {
            return null;
        }

        // Common pest name to model code mappings
        $pestMappings = [
            'Navel Orangeworm' => 'NOW',
            'Amyelois transitella' => 'NOW',
        ];

        // Try to find model code from pest name
        $modelCode = null;
        foreach ($pestMappings as $pest => $code) {
            if (stripos($pestName, $pest) !== false) {
                $modelCode = $code;
                break;
            }
        }

        // If no mapping found, try to extract from pest name format "Scientific Name - Common Name"
        if (!$modelCode && strpos($pestName, ' - ') !== false) {
            $parts = explode(' - ', $pestName);
            $commonName = trim($parts[1] ?? '');

            $modelCard = ModelCard::where('name', 'LIKE', "%{$commonName}%")
                ->orWhere('code', 'LIKE', "%{$commonName}%")
                ->first();

            if ($modelCard) {
                return $modelCard->id;
            }
        }

        // If we have a model code, look it up
        if ($modelCode) {
            $modelCard = ModelCard::where('code', $modelCode)->first();
            if ($modelCard) {
                return $modelCard->id;
            }
        }

        return null;
    }
}

