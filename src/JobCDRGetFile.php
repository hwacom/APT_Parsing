<?php
require '../vendor/autoload.php';
require_once 'service/CDRGetFile.php';

$cdrGet = new CDRGetFile();
$cdrGet->execute();