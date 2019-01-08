<?php
require '../vendor/autoload.php';

use Hwacom\APT_Parsing\service\EPDGSummary;

$summary = new EPDGSummary();
$summary->execute();