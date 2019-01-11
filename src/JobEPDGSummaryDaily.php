<?php
require '../vendor/autoload.php';
require_once 'service/EPDGSummary.php';

$summary = new EPDGSummary();
$summary->execute("DAY");