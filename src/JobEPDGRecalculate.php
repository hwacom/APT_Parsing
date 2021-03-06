<?php
require '../vendor/autoload.php';
require_once 'service/EPDGParsingRecalculate.php';

$recalculate = new EPDGParsingRecalculate();

//日期輸入格式: Y-m-d H:i；日期範圍限定必須在同一天內
$begin_date = $argv[1]; // [Ex]:2019-06-11 00:01
$end_date = $argv[2]; // [Ex]: 2019-06-11 00:15

if (!empty($begin_date) && !empty($end_date)) {
    $recalculate->execute($begin_date, $end_date);
}