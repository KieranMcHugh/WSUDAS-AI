<?php

namespace App\Console\Commands;

use App\Library\DasModels\LT50;
use Illuminate\Console\Command;
use JsonException;
use RuntimeException;
use Throwable;

class LT50Update extends Command
{
    protected $signature = 'update:lt50
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

            $rawInput = (string) $this->argument('json_body');
            $rawJson = $this->resolveRawJsonInput($rawInput);
            $payload = $this->decodePayload($rawJson);

            $predictions = LT50::predictFromPayloadStatic($payload, null);

            $this->line(json_encode($predictions));
            return self::SUCCESS;

        } catch (JsonException $e) {
            $this->error("Invalid JSON body: {$e->getMessage()}");
            return self::FAILURE;

        } catch (Throwable $e) {
            $this->error("Error calling LT50 API: {$e->getMessage()}");
            return self::FAILURE;
        }
    }

    private function resolveRawJsonInput(string $input): string
    {
        $candidate = trim($input);

        if ($candidate === '') {
            throw new RuntimeException('JSON body cannot be empty.');
        }

        $path = null;

        if (str_starts_with($candidate, '@')) {
            $path = substr($candidate, 1);
        } elseif (is_file($candidate)) {
            $path = $candidate;
        }

        if ($path !== null) {
            if (!is_readable($path)) {
                throw new RuntimeException("JSON file is not readable: {$path}");
            }

            $contents = file_get_contents($path);

            if ($contents === false) {
                throw new RuntimeException("Unable to read JSON file: {$path}");
            }

            return $this->normalizeJsonEncoding($contents);
        }

        return $this->normalizeJsonEncoding($input);
    }

    private function normalizeJsonEncoding(string $raw): string
    {
        if (strncmp($raw, "\xEF\xBB\xBF", 3) === 0) {
            return substr($raw, 3);
        }

        if (strncmp($raw, "\xFF\xFE", 2) === 0) {
            return mb_convert_encoding(substr($raw, 2), 'UTF-8', 'UTF-16LE');
        }

        if (strncmp($raw, "\xFE\xFF", 2) === 0) {
            return mb_convert_encoding(substr($raw, 2), 'UTF-8', 'UTF-16BE');
        }

        return $raw;
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
}
