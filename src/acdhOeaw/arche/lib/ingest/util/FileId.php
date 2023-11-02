<?php

/*
 * The MIT License
 *
 * Copyright 2023 zozlak.
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

use RuntimeException;
use SplFileInfo;

/**
 * Utility class for converting file paths into repository resource identifiers.
 *
 * @author zozlak
 */
class FileId {

    private const ENC_UTF8 = 'utf-8';

    /**
     * Detected operating system path enconding.
     */
    static private string $pathEncoding = '';

    /**
     * Tries to detect path encoding used by the operating system.
     * @throws RuntimeException
     */
    static private function pathToUtf8(string $path): string {
        if (empty(self::$pathEncoding)) {
            $ctype = setlocale(LC_CTYPE, '');
            if (!empty($ctype)) {
                $ctype = (string) preg_replace('|^.*[.]|', '', $ctype);
                if (is_numeric($ctype)) {
                    self::$pathEncoding = 'windows-' . $ctype;
                } else if (preg_match('|utf-?8|i', $ctype) || PHP_OS === 'Linux') {
                    self::$pathEncoding = 'utf-8';
                } else {
                    throw new RuntimeException('Operation system encoding can not be determined');
                }
            }
            // if there's nothing in LC_ALL, optimistically assume utf-8
            if (empty(self::$pathEncoding)) {
                self::$pathEncoding = self::ENC_UTF8;
            }
        }
        return self::$pathEncoding === self::ENC_UTF8 ? $path : (string) iconv(self::$pathEncoding, 'utf-8', $path);
    }

    private string $idPrefix;
    private int $defaultDirLen;

    public function __construct(string $idPrefix, string $defaultDir = '') {
        if (!empty($idPrefix) && substr($idPrefix, -1) !== '/') {
            $idPrefix .= '/';
        }
        $this->idPrefix      = $idPrefix;
        $this->defaultDirLen = $this->getDirLen($defaultDir);
    }

    public function getId(SplFileInfo | string $path,
                          string | null $directory = null): string {
        if ($path instanceof SplFileInfo) {
            $path = $path->getPathname();
        }
        $dirLen = $directory === null ? $this->defaultDirLen : $this->getDirLen($directory);
        $id     = self::pathToUtf8($path);
        $id     = str_replace('\\', '/', $id);
        $id     = substr($path, $dirLen);
        $id     = str_replace('%2F', '/', rawurlencode($id));
        return $this->idPrefix . $id;
    }

    private function getDirLen(string $dir): int {
        $dir = self::pathToUtf8($dir);
        if (!empty($dir) && substr($dir, -1) !== '/') {
            $dir .= '/';
        }
        return strlen($dir);
    }
}
