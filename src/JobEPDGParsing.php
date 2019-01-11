<?php
require '../vendor/autoload.php';
require_once 'service/EPDGParsingAndKpi.php';

$parsing = new EPDGParsingAndKpi();
$parsing->execute();