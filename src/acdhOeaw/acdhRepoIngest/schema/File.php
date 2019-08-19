<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\acdhRepoIngest\schema;

use RuntimeException;
use InvalidArgumentException;
use EasyRdf\Graph;
use EasyRdf\Resource;
use acdhOeaw\acdhRepoLib\BinaryPayload;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoIngest\schema\SchemaObject;
use acdhOeaw\acdhRepoIngest\util\Geonames;
use acdhOeaw\acdhRepoIngest\metaLookup\MetaLookupInterface;

/**
 * Description of File
 *
 * @author zozlak
 */
class File extends SchemaObject {

    /**
     * Detected operating system path enconding.
     * @var string
     */
    static private $pathEncoding;

    /**
     * Extracts relative path from a full path (by skipping cfg:containerDir)
     * @param string $fullPath
     * @param string $containerDir
     */
    static public function getRelPath(string $fullPath, string $containerDir): string {
        if (!strpos($fullPath, $containerDir) === 0) {
            throw new InvalidArgumentException('path is outside the container');
        }
        $containerDir = preg_replace('|/$|', '', $containerDir);
        return substr($fullPath, strlen($containerDir) + 1);
    }

    /**
     * Tries to detect path encoding used in the operating system by looking
     * into locale settings.
     * @throws RuntimeException
     */
    static private function detectPathEncoding() {
        if (self::$pathEncoding) {
            return;
        }
        foreach (explode(';', setlocale(LC_ALL, 0)) as $i) {
            $i = explode('=', $i);
            if ($i[0] === 'LC_CTYPE') {
                $tmp = preg_replace('|^.*[.]|', '', $i[1]);
                if (is_numeric($tmp)) {
                    self::$pathEncoding = 'windows-' . $tmp;
                    break;
                } else if (preg_match('|utf-?8|i', $tmp)) {
                    self::$pathEncoding = 'utf-8';
                    break;
                } else {
                    throw new RuntimeException('Operation system encoding can not be determined');
                }
            }
        }
    }

    /**
     * Sanitizes file path - turns all \ into / and assures it is UTF-8 encoded.
     * @param string $path
     * @param string $pathEncoding
     * @return string
     */
    static public function sanitizePath(string $path,
                                        string $pathEncoding = null): string {
        if ($pathEncoding === null) {
            self::detectPathEncoding();
            $pathEncoding = self::$pathEncoding;
        }
        $path = iconv($pathEncoding, 'utf-8', $path);
        $path = str_replace('\\', '/', $path);
        return $path;
    }

    /**
     * File path
     * @var string 
     */
    private $path;

    /**
     * Metadata lookup object to be used for metadata resoultion
     * @var \acdhOeaw\util\metaLookup\metaLookupInterface
     * @see setMetaLookup()
     */
    private $metaLookup;

    /**
     * should metadata operations fail when no external metadata can be found
     * @var bool
     */
    private $metaLookupRequire = false;

    /**
     * RDF class to be set as a this file's type.
     * @var string
     */
    private $class;

    /**
     * URI of the resource which should be set as this file's parent
     * (cfg::fedoraRelProp is used).
     * @var string
     */
    private $parent;

    /**
     * Creates an object representing a file (or a directory) in a filesystem.
     * @param Fedora $repo repository connection object
     * @param type $id file path
     * @param string $class RDF class to be used as a repository resource type
     * @param string $parent URI of a repository resource being parent of 
     *   created one
     */
    public function __construct(Repo $repo, string $id, string $class = null,
                                string $parent = null) {
        $this->repo   = $repo;
        $this->path   = $id;
        $this->class  = $class;
        $this->parent = $parent;

        $prefix = $this->repo->getSchema()->ingest->containerToUriPrefix;
        $id     = self::sanitizePath($id);
        $id     = self::getRelPath($id, $this->repo->getSchema()->ingest->containerDir);
        $id     = str_replace('%2F', '/', rawurlencode($id));
        parent::__construct($repo, $prefix . $id);
    }

    /**
     * Creates RDF metadata for a file.
     * 
     * Can use metadata lookup mechanism - see the setMetaLookup() method.
     * @return Resource
     * @see setMetaLookup()
     */
    public function getMetadata(): Resource {
        $idProp    = $this->repo->getSchema()->id;
        $relProp   = $this->repo->getSchema()->parent;
        $titleProp = $this->repo->getSchema()->label;

        $graph = new Graph();
        $meta  = $graph->resource('.');

        $meta->addResource($idProp, $this->getId());

        if ($this->class != '') {
            $meta->addResource('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', $this->class);
        }

        if ($this->parent) {
            $meta->addResource($relProp, $this->parent);
        }

        $location = self::getRelPath(self::sanitizePath($this->path), $this->repo->getSchema()->ingest->containerDir);
        $meta->addLiteral($this->repo->getSchema()->ingest->location, $location);

        $meta->addLiteral($titleProp, basename($this->path));
        $meta->addLiteral('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename', basename($this->path));
        if (is_file($this->path)) {
            $mime = $this->getMime();
            if ($mime) {
                $meta->addLiteral('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType', $mime);
            }
            $meta->addLiteral($this->repo->getSchema()->binarySize, filesize($this->path));
        }

        if ($this->metaLookup) {
            $addMeta = $this->metaLookup->getMetadata($this->path, $meta, $this->metaLookupRequire);
            $meta->merge($addMeta, [$idProp]);
        }

        return $meta;
    }

    /**
     * Sets a metadata lookup object to be used for searching for file's
     * metadata which can not be automatically derived from the file itself.
     * 
     * Metadata found using metadata lookup have precedense over metadata
     * automatically derived from the file.
     * @param MetaLookupInterface $metaLookup metadata lookup object
     * @param bool $require should metadata operations fail when no external
     *   metadata can be found
     */
    public function setMetaLookup(MetaLookupInterface $metaLookup,
                                  bool $require = false) {
        $this->metaLookup        = $metaLookup;
        $this->metaLookupRequire = $require;
    }

    /**
     * Returns file's mime type
     * @return string
     */
    public function getMime(): ?string {
        $mime = \GuzzleHttp\Psr7\mimetype_from_filename(basename($this->path));
        if ($mime === null) {
            $mime = mime_content_type($this->path);
        }
        $mime = empty($mime) ? null : $mime;
        return $mime;
    }

    /**
     * Returns file path (cause path is supported by the `Fedora->create()`). 
     * @return string
     */
    protected function getBinaryData(): ?BinaryPayload {
        if (is_dir($this->path)) {
            return null;
        }
        return new BinaryPayload(null, $this->path);
    }

    /**
     * Merges metadata coming from the Fedora and generated by the class.
     * 
     * Preserves the resource title if it already exists.
     * 
     * @param Resource $current current Fedora resource metadata
     * @param Resource $new metadata generated by the class
     * @return Resource final metadata
     */
    protected function mergeMetadata(Resource $current, Resource $new): Resource {
        $idProp    = $this->repo->getSchema()->id;
        $titleProp = $this->repo->getSchema()->label;

        // title handling logic:
        // if title is not provided by an external metadata (also when there is
        // no external metadata) and current
        $oldTitle = $current->getLiteral($titleProp);
        $extTitle = null;

        $meta = $current->merge($new, [$idProp]);

        if ($this->metaLookup) {
            $extMeta  = $this->metaLookup->getMetadata($this->path, $new, $this->metaLookupRequire);
            $extTitle = $extMeta->getLiteral($titleProp);
        }
        if ($oldTitle !== null && $extTitle === null) {
            $meta->delete($titleProp);
            $meta->addLiteral($titleProp, $oldTitle, $oldTitle->getLang());
        }

        Geonames::standardizeProperty($meta, $idProp);

        return $meta;
    }

}
