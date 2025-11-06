<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Facades\DB;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class InsertContourDataJob implements ShouldQueue
{
    use InteractsWithQueue, Queueable, SerializesModels;

    protected string $table;
    protected array $data;
    protected ?int $regionId;

    public function __construct(string $table, array $data, ?int $regionId = null)
    {
        $this->table = $table;
        $this->data = $data;
        $this->regionId = $regionId;
    }

    public function handle()
    {
        if ($this->table === 'contour_locations') {
            $insertData = array_map(fn($row) => [
                'name' => $row['name'],
                'lat' => $row['lat'],
                'lng' => $row['lng'],
                'survey_year' => date('Y'),
                'contour_region_id' => $this->regionId,
                'created_at' => now(),
                'created_by' => 1,
            ], $this->data);
        } else {
            // For contour_trap_counts
            $insertData = $this->data;
        }

        DB::table("service_models.{$this->table}")->insert($insertData);
    }
}
