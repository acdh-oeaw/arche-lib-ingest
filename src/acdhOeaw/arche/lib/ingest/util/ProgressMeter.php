<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\lib\ingest\util;

/**
 * Helper class for displaying progress meter
 *
 * @author zozlak
 */
class ProgressMeter {

    /**
     * 
     * @var array<string, array<int, int>>
     */
    static private $counters = [];

    static public function init(string $id, int $max): void {
        self::$counters[$id] = [0, $max];
    }

    static public function increment(?string $id, string $format = ''): int {
        if ($id === null) {
            return 0;
        }
        self::$counters[$id][0]++;
        return self::$counters[$id][0];
    }

    static public function format(?string $id, ?int $n, string $format): string {
        if ($id === null) {
            return '';
        }
        $N = self::$counters[$id][1];
        return str_replace(['{n}', '{t}', '{p}'], [(string) $n, (string) $N, (string) round(100 * $n / $N)], $format);
    }
}
