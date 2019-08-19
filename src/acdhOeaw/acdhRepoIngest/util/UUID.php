<?php

/*
 * The MIT License
 *
 * Copyright 2016 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\acdhRepoIngest\util;

/**
 * Generates UUIDs 
 * (see https://en.wikipedia.org/wiki/Universally_unique_identifier)
 *
 * @author zozlak
 */
class UUID {

    /**
     * Generates a v4 (random) UUID
     * @return string
     */
    static public function v4(): string {
        $n = array();
        for ($i = 0; $i < 8; $i++) {
            $n[$i] = mt_rand(0, 0xffff);
        }
        $n[3] = $n[3] | 0x4000;
        $n[4] = $n[4] | 0x8000;
        $format = '%04x%04x-%04x-%04x-%04x-%04x%04x%04x';
        return sprintf($format, $n[0], $n[1], $n[2], $n[3], $n[4], $n[5], $n[6], $n[7]);
    }

}
