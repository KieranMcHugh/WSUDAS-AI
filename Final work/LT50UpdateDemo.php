<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use JsonException;
use RuntimeException;
use Throwable;

class <?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use JsonException;
use RuntimeException;
use Throwable;

class LT50UpdateDemo extends Command
{
    protected $signature = 'predict:lt50
                            {station_id : Station ID}
                            {json_body : Raw JSON body to send to the LT50 API}';

    protected $description = 'Send a raw JSON body to the LT50 predict API for a station.';

    public function handle(): int
    {
        try {
            $stationId = (int) $this->argument('station_id');

            if ($stationId <= 0) {
                $this->error('station_id must be a positive integer.');
                return self::FAILURE;
            }

            $rawJson = (string) $this->argument('json_body');
            $payload = $this->decodePayload($rawJson);

            $response = Http::timeout((int) config('services.lt50.timeout', 120))
                ->post($this->getFastApiBaseUrl() . '/predict', $payload);

            if ($response->failed()) {
                $this->error('LT50 API request failed:');
                $this->line($response->body());
                return self::FAILURE;
            }

            $this->line($response->body());
            return self::SUCCESS;

        } catch (JsonException $e) {
            $this->error("Invalid JSON body: {$e->getMessage()}");
            $this->line('');
            $this->line('PowerShell usage:');
            $this->line('php artisan predict:lt50 1 \'{"latitude":43.0,"cultivar":"Riesling","data":[{"date":"2025-11-01","tmin":34.0,"tmax":52.0}]}\'');
            return self::FAILURE;

        } catch (Throwable $e) {
            $this->error("Error calling LT50 API: {$e->getMessage()}");
            return self::FAILURE;
        }
    }

    private function decodePayload(string $rawJson): array
    {
        $trimmed = trim($rawJson);

        if ($trimmed === '') {
            throw new RuntimeException('JSON body cannot be empty.');
        }

        $payload = json_decode($trimmed, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($payload)) {
            throw new RuntimeException('JSON body must decode to an object or array.');
        }

        foreach (['latitude', 'cultivar', 'data'] as $requiredField) {
            if (!array_key_exists($requiredField, $payload)) {
                throw new RuntimeException("Missing required JSON field: {$requiredField}");
            }
        }

        if (!is_numeric($payload['latitude'])) {
            throw new RuntimeException('latitude must be numeric.');
        }

        if (!is_string($payload['cultivar']) || trim($payload['cultivar']) === '') {
            throw new RuntimeException('cultivar must be a non-empty string.');
        }

        if (!is_array($payload['data']) || count($payload['data']) === 0) {
            throw new RuntimeException('data must be a non-empty array.');
        }

        foreach ($payload['data'] as $index => $record) {
            if (!is_array($record)) {
                throw new RuntimeException("data[{$index}] must be an object.");
            }

            foreach (['date', 'tmin', 'tmax'] as $field) {
                if (!array_key_exists($field, $record)) {
                    throw new RuntimeException("Missing required field data[{$index}].{$field}");
                }
            }
        }

        return $payload;
    }

    private function getFastApiBaseUrl(): string
    {
        return rtrim((string) config('services.lt50.base_url', 'http://0.0.0.0:8000'), '/');
    }
}
 extends Command
{
    protected $signature = 'predict:lt50
                            {station_id : Station ID}
                            {json_body : Raw JSON body to send to the LT50 API}';

    protected $description = 'Send a raw JSON body to the LT50 predict API for a station.';

    public function handle(): int
    {
        try {
            $stationId = (int) $this->argument('station_id');

            if ($stationId <= 0) {
                $this->error('station_id must be a positive integer.');
                return self::FAILURE;
            }

            $rawJson = (string) $this->argument('json_body');
            $payload = $this->decodePayload($rawJson);

            $response = Http::timeout((int) config('services.lt50.timeout', 120))
                ->post($this->getFastApiBaseUrl() . '/predict', $payload);

            if ($response->failed()) {
                $this->error('LT50 API request failed:');
                $this->line($response->body());
                return self::FAILURE;
            }

            $this->line($response->body());
            return self::SUCCESS;

        } catch (JsonException $e) {
            $this->error("Invalid JSON body: {$e->getMessage()}");
            $this->line('');
            $this->line('PowerShell usage:');
            $this->line('php artisan predict:lt50 1 \'{"latitude":43.0,"cultivar":"Riesling","data":[{"date":"2025-11-01","tmin":34.0,"tmax":52.0}]}\'');
            return self::FAILURE;

        } catch (Throwable $e) {
            $this->error("Error calling LT50 API: {$e->getMessage()}");
            return self::FAILURE;
        }
    }

    private function decodePayload(string $rawJson): array
    {
        $trimmed = trim($rawJson);

        if ($trimmed === '') {
            throw new RuntimeException('JSON body cannot be empty.');
        }

        $payload = json_decode($trimmed, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($payload)) {
            throw new RuntimeException('JSON body must decode to an object or array.');
        }

        foreach (['latitude', 'cultivar', 'data'] as $requiredField) {
            if (!array_key_exists($requiredField, $payload)) {
                throw new RuntimeException("Missing required JSON field: {$requiredField}");
            }
        }

        if (!is_numeric($payload['latitude'])) {
            throw new RuntimeException('latitude must be numeric.');
        }

        if (!is_string($payload['cultivar']) || trim($payload['cultivar']) === '') {
            throw new RuntimeException('cultivar must be a non-empty string.');
        }

        if (!is_array($payload['data']) || count($payload['data']) === 0) {
            throw new RuntimeException('data must be a non-empty array.');
        }

        foreach ($payload['data'] as $index => $record) {
            if (!is_array($record)) {
                throw new RuntimeException("data[{$index}] must be an object.");
            }

            foreach (['date', 'tmin', 'tmax'] as $field) {
                if (!array_key_exists($field, $record)) {
                    throw new RuntimeException("Missing required field data[{$index}].{$field}");
                }
            }
        }

        return $payload;
    }

    private function getFastApiBaseUrl(): string
    {
        return rtrim((string) config('services.lt50.base_url', 'http://0.0.0.0:8000'), '/');
    }
}
