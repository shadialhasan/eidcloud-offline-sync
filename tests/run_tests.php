<?php

declare(strict_types=1);

// Zero-dependency automated test runner
spl_autoload_register(function (string $class) {
    $prefix = 'EidCloud\\OfflineSync\\';
    $baseDir = __DIR__ . '/../src/';

    $len = strlen($prefix);
    if (strncmp($prefix, $class, $len) !== 0) {
        return;
    }

    $relativeClass = substr($class, $len);

    if (str_starts_with($relativeClass, 'Tests\\')) {
        $file = __DIR__ . '/' . substr($relativeClass, 6) . '.php';
    } else {
        $file = $baseDir . str_replace('\\', '/', $relativeClass) . '.php';
    }

    if (file_exists($file)) {
        require_once $file;
    }
});

require_once __DIR__ . '/OfflineSyncTest.php';

$suite = new EidCloud\OfflineSync\Tests\OfflineSyncTest();
$success = $suite->runAll();

exit($success ? 0 : 1);
