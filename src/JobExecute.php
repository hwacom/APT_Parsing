<?php
require '../vendor/autoload.php';

use Hwacom\APT_Parsing\BulkStatsParsingAndKpi;

$parsing = new BulkStatsParsingAndKpi();
$parsing->execute();
