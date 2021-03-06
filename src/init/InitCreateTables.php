<?php
require '../../vendor/autoload.php';

use Hwacom\APT_Parsing\utils\LoadTemplate;


$file_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/init/ePDG Counter Templete.csv";
//$file_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/init/test.csv";
$file_uk_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/init/unique_keys.csv";
$file_summary_uk_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/init/summary_unique_keys.csv";
$file_kpi_path = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/init/KPI_init.csv";

$init_obj = new LoadTemplate();


//$init_obj->createKpiFormulaTable($file_kpi_path, "A");    //ミKPI table & KPI_FORMULA_SETTING insert
$init_obj->createKpiFormulaTable($file_kpi_path, "T");    //ミKPI table
//$init_obj->createKpiFormulaTable($file_kpi_path, "F");    //ミKPI_FORMULA_SETTING insert

//$init_obj->createTable($file_path);

/*
$init_obj->createTable($file_path);
$init_obj->createConstraints($file_uk_path);                        // Main TABLE
$init_obj->createConstraints($file_uk_path, "TEMP");                // Temp TABLE
$init_obj->createConstraints($file_summary_uk_path, "SUMMARY");     // Summary TABLE
*/

//$init_obj->composeSysTableMapping($file_path);

