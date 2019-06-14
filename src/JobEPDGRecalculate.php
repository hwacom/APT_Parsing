<?php
require '../vendor/autoload.php';
require_once 'service/EPDGParsingRecalculate.php';

$recalculate = new EPDGParsingRecalculate();

//�����J�榡: Y-m-d H:i�F����d�򭭩w�����b�P�@�Ѥ�
$begin_date = $argv[1]; // [Ex]:2019-06-11 00:01
$end_date = $argv[2]; // [Ex]: 2019-06-11 00:15

if (!empty($begin_date) && !empty($end_date)) {
    $recalculate->execute($begin_date, $end_date);
}