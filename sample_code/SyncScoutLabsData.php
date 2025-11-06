<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class SyncScoutLabsData extends Command
{
    protected $signature = 'scoutlabs:sync';
    protected $description = 'Sync Scout Labs data into the Contour system';

    public function handle()
    {
        // wrap as transaction
        DB::beginTransaction();

        try {
            // static region name (for the moment)
            $regionName = 'Scout Labs';

            // check if the region exists
            $region = DB::table('service_models.contour_regions')
                ->where('name', $regionName)
                ->first();

            // if region does not exist, add it
            if (!$region) {
                $regionId = DB::table('service_models.contour_regions')->insertGetId([
                    'name' => $regionName,
                    'active' => 1,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);
                $this->info("Created new region: {$regionName}");
            } else {
                $regionId = $region->id;
            }

            // get new locations that are not already in contour_locations
            $newLocations = DB::table('service_weather.scout_labs_traps as wt')
                ->leftJoin('service_models.contour_locations as ml', function ($join) use ($regionId) {
                    $join->on('ml.name', '=', 'wt.name')
                        ->where('ml.contour_region_id', $regionId)
                        ->where('ml.survey_year', date('Y'));
                })
                ->whereNull('ml.id')
                ->whereNotNull('wt.lat')
                ->whereNotNull('wt.lng')
                ->select('wt.name', 'wt.lat', 'wt.lng')
                ->get();

            // insert missing locations (I tried to do so in chunks here)
            if ($newLocations->isNotEmpty()) {
                $chunks = $newLocations->chunk(500);
                foreach ($chunks as $chunk) {
                    $insertData = $chunk->map(fn($row) => [
                        'name' => $row->name,
                        'lat' => $row->lat,
                        'lng' => $row->lng,
                        'survey_year' => date('Y'),
                        'contour_region_id' => $regionId,
                        'created_at' => now(),
                        'created_by' => 1,
                    ])->toArray();

                    DB::table('service_models.contour_locations')->insert($insertData);
                }
                $this->info("Inserted {$newLocations->count()} new locations.");
            } else {
                $this->info("No new locations to insert.");
            }

            // insert trap counts over the past 7 days
            $from = now()->subDays(7)->toDateString();
            $to   = now()->toDateString();

            $trapCounts = DB::table('service_weather.scout_labs_records as wr')
                ->join('service_weather.scout_labs_traps as wt', 'wt.trap_id', '=', 'wr.trap_id')
                ->join('service_models.contour_locations as ml', function ($join) use ($regionId) {
                    $join->on('ml.name', '=', 'wt.name')
                        ->where('ml.contour_region_id', $regionId)
                        ->where('ml.survey_year', date('Y'));
                })
                ->whereBetween('wr.recorded_at', [$from, $to])
                ->whereNotExists(function ($query) {
                    $query->select(DB::raw(1))
                        ->from('service_models.contour_trap_counts as c')
                        ->whereColumn('c.location_id', 'ml.id')
                        ->whereRaw('c.survey_date = DATE(wr.recorded_at)');
                })
                ->selectRaw('ml.id as location_id, DATE(wr.recorded_at) as survey_date, COALESCE(wr.detection_count, 0) as trap_count, NOW() as created_at')
                ->get();

            if ($trapCounts->isNotEmpty()) {
                $chunks = $trapCounts->chunk(500);
                foreach ($chunks as $chunk) {
                    $insertData = $chunk->toArray();
                    DB::table('service_models.contour_trap_counts')->insert($insertData);
                }
                $this->info("Inserted {$trapCounts->count()} new trap count records.");
            } else {
                $this->info("No new trap counts to insert.");
            }

            DB::commit();
        } catch (\Exception $e) {
            DB::rollBack();
            $this->error("Sync failed: " . $e->getMessage());
            \Log::error("Scout Labs sync error", ['exception' => $e]);
        }
    }
}
