<?php

namespace App\Library\DasModels;

use App\Exceptions\NoDailyRecordsFoundException;
use App\ORM\ModelRecords\ModelRecord;
use App\ORM\Weather\Records\DailyRecord;
use Carbon\Carbon;
use Illuminate\Support\Facades\Http;
use InvalidArgumentException;

class LT50 extends DasModel
{
    public function update(
        int $stationId,
        ?ModelRecord $lastUpdatedRecord,
        Carbon $to,
        ?Carbon $now = null
    ): ?ModelRecord {
        $now = ($now ?? now())->startOfDay();

        [$from, $to] = $this->resolveDateRange($stationId, $lastUpdatedRecord, $now);

        $dailyRecords = DailyRecord::getFromTo($from, $to, $stationId);

        if ($dailyRecords->count() === 0) {
            throw new NoDailyRecordsFoundException($stationId, $this->modelCard->id, $from, $to);
        }

        $dailyPayload = $this->buildDailyPayload($dailyRecords);
        $predictions = $this->predictFromPayload($dailyPayload, $stationId);

        return $this->latestUpdate($stationId, $this->modelCard->id);
    }

    private function resolveDateRange(
        int $stationId,
        ?ModelRecord $lastUpdatedRecord,
        Carbon $now
    ): array {
        $from = $lastUpdatedRecord ? $lastUpdatedRecord->tstamp->copy()->addDay() : $now->copy();
        $to = $now->copy()->addDays(14);

        return [$from, $to];
    }

    public function predictFromPayload(array $payload, ?int $stationId = null): array
    {
        return self::predictFromPayloadStatic($payload, $stationId);
    }

    public static function predictFromPayloadStatic(array $payload, ?int $stationId = null): array
    {
        $requestPayload = self::normalizePredictRequestPayload($payload, $stationId);
        return self::runPredictionApiStatic($requestPayload);
    }

    private function buildDailyPayload(iterable $dailyRecords): array
    {
        $dailyPayload = [];

        foreach ($dailyRecords as $record) {
            $dailyPayload[] = [
                'date' => $record->tstamp->toDateString(),
                'tmin' => $record->temperature_min,
                'tmax' => $record->temperature_max,
            ];
        }

        return $dailyPayload;
    }

    private static function normalizePredictRequestPayload(array $payload, ?int $stationId = null): array
    {
        $hasRequestShape = array_key_exists('data', $payload);
        $requestPayload = $hasRequestShape ? $payload : ['data' => $payload];

        if (!array_key_exists('data', $requestPayload) || !is_array($requestPayload['data'])) {
            throw new InvalidArgumentException('LT50 payload must contain a data array.');
        }

        if (!array_key_exists('latitude', $requestPayload) || !is_numeric($requestPayload['latitude'])) {
            $requestPayload['latitude'] = self::resolveLatitude($stationId);
        }

        if (!array_key_exists('cultivar', $requestPayload) || !is_string($requestPayload['cultivar']) || trim($requestPayload['cultivar']) === '') {
            $requestPayload['cultivar'] = (string) config('services.lt50.default_cultivar', 'Riesling');
        }

        return [
            'latitude' => (float) $requestPayload['latitude'],
            'cultivar' => trim((string) $requestPayload['cultivar']),
            'data' => $requestPayload['data'],
        ];
    }

    private static function resolveLatitude(?int $stationId): float
    {
        return (float) config('services.lt50.default_latitude', 43.0);
    }

    private static function runPredictionApiStatic(array $requestPayload): array
    {
        $response = Http::timeout((int) config('services.lt50.timeout', 120))
            ->post(self::getFastApiBaseUrlStatic() . '/predict', $requestPayload);

        if ($response->failed()) {
            $error = $response->json('detail') ?? $response->body();
            throw new \Exception("LT50 API request failed: {$error}");
        }

        $predictions = $response->json('predictions');

        if (!is_array($predictions)) {
            return [];
        }

        return $predictions;
    }

    private static function getFastApiBaseUrlStatic(): string
    {
        return rtrim((string) config('services.lt50.base_url', 'http://0.0.0.0:8000'), '/');
    }
}
