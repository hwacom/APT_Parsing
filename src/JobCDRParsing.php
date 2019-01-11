<?php
require '../vendor/autoload.php';
require_once 'service/CDRParsing.php';

$cdrParsing = new CDRParsing();
$cdrParsing->execute();