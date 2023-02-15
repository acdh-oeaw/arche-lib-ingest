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

namespace acdhOeaw\arche\lib\ingest;

use RuntimeException;
use Redmine\Client\NativeCurlClient;

/**
 * Helper for updating the ACDH Redmine issues documenting the curation process
 *
 * @author zozlak
 */
class Redmine {

    const SUBTASKS = [
        'Virus scan'                                 => [
            "Virus scan failed",
            "Virus scan performed successfully",
        ],
        'Run repo-file-checker'                      => [
            "repo-file-checker found errors",
            "repo-file-checker exposed no error",
        ],
        'Prepare Ingest Files'                       => [
            "Failed to prepare files for an ingestion",
            "Successfully preparred files for an ingestion",
        ],
        'Upload AIP to Curation Instance (Minerva)'  => [
            "Failed to upload data to the curation instance",
            "Successfully uploaded data to the curation instance",
        ],
        'Upload AIP to Productive Instance (Apollo)' => [
            "Failed to upload data to the production instance",
            "Successfully uploaded data to the production instance",
        ],
        'Create PID'                                 => [
            "Failed to create PIDs",
            "Successfully created PIDs",
        ],
    ];

    private string $apiBase;
    private NativeCurlClient $redmine;

    public function __construct(string $apiBase, string $pswdOrToken,
                                string $user = '') {
        $this->apiBase = $apiBase;
        if (empty($user)) {
            $this->redmine = new NativeCurlClient($apiBase, $pswdOrToken);
        } else {
            $this->redmine = new NativeCurlClient($apiBase, $pswdOrToken, $user);
        }
    }

    /**
     * 
     * @param int $issueId
     * @param string $subtask
     * @param bool $status
     * @param string|null $issueStatus
     * @param int|null $done
     * @param string $message
     * @param bool $append
     * @return void
     * @throws RuntimeException
     */
    public function updateIssue(int $issueId, string $subtask, bool $status,
                                ?string $issueStatus = null, ?int $done = null,
                                string $message = '', bool $append = false): void {
        $issuesApi = $this->redmine->getApi('issue');
        // check main redmine issue
        $issue     = $issuesApi->show($issueId);
        if (!is_array($issue)) {
            throw new RuntimeException("Can't access $this->apiBase/issues/$issueId. Check provided credentials and the redmine issue ID.");
        }
        $issue = $issue['issue'];
        if ($issue['subject'] !== $subtask) {
            // find proper subtask
            $issues = $issuesApi->all([
                'parent_id' => $issueId,
                'status_id' => '*',
                'limit'     => 100,
            ]);
            foreach ($issues['issues'] ?? [] as $issue) {
                if ($issue['subject'] == $subtask) {
                    break;
                } else {
                    $subissues = $issuesApi->all([
                        'parent_id' => $issue['id'],
                        'status_id' => '*',
                        'subject'   => $subtask,
                    ]);
                    if (is_array($subissues) && count($subissues['issues']) === 1) {
                        $issue = $subissues['issues'][0];
                        break;
                    }
                }
            }
        }
        if ($issue['subject'] !== $subtask) {
            throw new RuntimeException("Can't find the '$subtask' subtask. Please check your redmine issues structure.");
        }
        $issueId = $issue['id'];
        // update the issue
        if (!empty($issueStatus)) {
            $issuesApi->setIssueStatus($issueId, $issueStatus);
        }
        if (!empty($done)) {
            $issuesApi->update($issueId, ['done_ratio' => $done]);
        }

        if ($append) {
            $message = self::SUBTASKS[$subtask][(int) $status] . rtrim("\n\n" . $message);
        } elseif (empty($message)) {
            $message = self::SUBTASKS[$subtask][(int) $status];
        }
        $issuesApi->addNoteToIssue($issueId, $message);
    }
}
