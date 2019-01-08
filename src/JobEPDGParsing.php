<?php
require '../vendor/autoload.php';

use Hwacom\APT_Parsing\service\EPDGParsingAndKpi;

$parsing = new EPDGParsingAndKpi();
$parsing->execute();
