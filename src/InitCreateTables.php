<?php
require '../vendor/autoload.php';

use Hwacom\APT_Parsing_Core\BulkStatsParsingAndKpi;


use Hwacom\APT_Parsing_Core\utils\LoadTemplate;


$file_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG/Files/init/ePDG Counter Templete.csv";
//$file_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG/Files/init/test.csv";
$file_uk_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG/Files/init/unique_keys.csv";
$file_summary_uk_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG/Files/init/summary_unique_keys.csv";
$file_kpi_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG/Files/init/KPI_init.csv";

$init_obj = new LoadTemplate();

$init_obj->createKpiFormulaTable($file_kpi_path);
/*
 $init_obj->createTable($file_path);
/*
$init_obj->createTable($file_path);
$init_obj->createConstraints($file_uk_path, false);
$init_obj->createConstraints($file_summary_uk_path, true);
*/
/*
$init_obj->composeSysTableMapping($file_path);
*/

/*
$parsing = new BulkStatsParsingAndKpi();
$parsing->execute();
*/