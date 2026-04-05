<?php

namespace App\Library\DasModels;

use App\Exceptions\NoDailyRecordsFoundException;
use App\ORM\ModelRecords\ModelRecord;
use App\ORM\Weather\Records\DailyRecord;
use Carbon\Carbon;
use Illuminate\Support\Facades\Http;

class LT50 extends DasModel
{
    public function __construct(string $country, string $state)
    {
        parent::__construct($country, $state);
    }

    public function update(
        int $stationId,
        ModelRecord $lastUpdatedRecord,
        Carbon $to,
        ?Carbon $now = null
    ): ?ModelRecord {
        $from = $lastUpdatedRecord->tstamp;
        $to = now()->addDays(14);

        $dailyRecords = DailyRecord::getFromTo($from, $to, $stationId);

        if ($dailyRecords->count() === 0) {
            throw new NoDailyRecordsFoundException($stationId, $this->modelCard->id, $from, $to);
        }

        $predictions = $this->runPredictionApi($dailyRecords);

        if (count($predictions) === 0) {
            return $this->latestUpdate($stationId, $this->modelCard->id);
        }


        return $this->latestUpdate($stationId, $this->modelCard->id);
    }

    private function runPredictionApi(iterable $dailyRecords): array
    {
        $dailyPayload = [];
        foreach ($dailyRecords as $record) {
            $dailyPayload[] = [
                'date' => $record->tstamp->toDateString(),
                'tmin' => $record->temperature_min,
                'tmax' => $record->temperature_max,
            ];
        }

        //call config, not env, also put the timeout in config
        $response = Http::timeout((int)env('LT50_API_TIMEOUT', 120))
            ->post($this->getFastApiBaseUrl().'/predict', $dailyPayload);

        if ($response->failed()) {
            $error = $response->json('detail') ?? $response->body();
            throw new \Exception("LT50 API request failed: {$error}");
            //catch exception on console command
        }

        $predictions = $response->json('predictions');

        if (!is_array($predictions)) {
            return [];
        }

        return $predictions;
    }

    private function getFastApiBaseUrl(): string
    {
        return rtrim((string)env('LT50_API_BASE_URL', 'http://127.0.0.1:8000'), '/');
    }
}
