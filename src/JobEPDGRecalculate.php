<?php
require '../vendor/autoload.php';
require_once 'service/EPDGParsingRecalculate.php';

$recalculate = new EPDGParsingRecalculate();

//�����J�榡: Y-m-d H:i�F����d�򭭩w�����b�P�@�Ѥ�
$begin_date = "2019-06-11 00:01";
$end_date = "2019-06-11 00:15";

$recalculate->execute($begin_date, $end_date);