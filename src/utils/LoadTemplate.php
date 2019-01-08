<?php
namespace Hwacom\APT_Parsing\utils;

class LoadTemplate
{
    //private $default_type = "decimal(11,2)";
    private $default_type = "decimal(18,2)";
    private $specify_type_fields = array(
        "cpu0-name"									=> "string",
        "cpu1-name"									=> "string",
        "cpu2-name"									=> "string",
        "cpu3-name"									=> "string",
        "servname"									=> "string",
        "vpnname"									=> "string",
        "servid"									=> "string",
        "vpnid"										=> "string",
        "vpnname"									=> "string",
        "ipaddr"									=> "string",
        "card"										=> "string",
        "port"										=> "string",
        "servertype"								=> "string",
        "group"										=> "string",
        "peer"										=> "string",
        "tun-sent-delsessrespdeniedMandIEIncorrect"	=> "string",
        "hostname"									=> "string",
        "type"										=> "string",
        "sub_type"									=> "string"
    );
    private $specify_aggregation_fields = array(
        "cpu0-name"                                 => "[VALUE]",
        "cpu1-name"                                 => "[VALUE]",
        "cpu2-name"                                 => "[VALUE]",
        "cpu3-name"                                 => "[VALUE]"
    );
    private $fields_type = array(
        "epochtime"                                 => "int(11)",
        "localdate"                                 => "int(11)",
        "localtime"                                 => "int(11)",
        "uptime"                                    => "int(11)",
        /*
        "15avg-memused"								=> "decimal(15,2)",
        "15peak-memused"							=> "decimal(15,2)",
        "1avg-memused"								=> "decimal(15,2)",
        "5avg-memused"								=> "decimal(15,2)",
        "5peak-memused"								=> "decimal(15,2)",
        */
        "cpu0-name"									=> "varchar(20)",
        "cpu1-name"									=> "varchar(20)",
        "cpu2-name"									=> "varchar(20)",
        "cpu3-name"									=> "varchar(20)",
        /*
        "maxrate"									=> "decimal(16,2)",
        "mcast_inpackets"							=> "decimal(16,2)",
        "rxbytes"									=> "decimal(16,2)",
        "rxpackets"									=> "decimal(16,2)",
        */
        "servname"									=> "varchar(50)",
        /*
        "txbytes"									=> "decimal(16,2)",
        "txpackets"									=> "decimal(16,2)",
        "ucast_inpackets"							=> "decimal(16,2)",
        "ucast_outpackets"							=> "decimal(16,2)",
        */
        "vpnname"									=> "varchar(20)",
        "servid"									=> "varchar(50)",
        "vpnid"										=> "varchar(50)",
        "vpnname"									=> "varchar(50)",
        "ipaddr"									=> "varchar(50)",
        "card"										=> "varchar(50)",
        "port"										=> "varchar(50)",
        "servertype"								=> "varchar(50)",
        "group"										=> "varchar(50)",
        "peer"										=> "varchar(50)",
        "tun-sent-delsessrespdeniedMandIEIncorrect"	=> "varchar(50)"
    );
    private $aggregation_fields = array("_sum", "_avg", "_max", "_min");
    private $key_fields = array(
        "epochtime",
        "localdate",
        "localtime",
        "uptime",
        "card",
        "port",
        "vpnname",
        "vpnid",
        "servname",
        "servid",
        "ipaddr"
    );
    private $csv_column_mapping = array(
        0 => "hostname",
        1 => "type",
        2 => "sub_type"
    );
    private $sql_array = array();
    private $file_path;
    
    public function __construct() {
        
    }
    
    public function __destruct() {
        
    }
    
    /**
     * **提供以Template檔轉換出 CREATE TABLE 的SQL
     * @param String $file_path
     */
    public function createTable($file_path) {
        $file = fopen($file_path,"r");
        
        $row_idx = 0;
        $ignore_rows = 1;
        while(!feof($file))
        {
            $fields = fgetcsv($file);
            
            if (empty($fields)) {
                continue;
            }
            
            if ($row_idx < $ignore_rows) {
                $row_idx++;
                continue;
            }
            
            $fields = array_values(array_filter($fields));
            if (empty($fields)) {
                continue;
            }
            
            $sql = "create table ";
            $sql_aggregation = "create table ";
            
            $table_name = $this->transTableName($fields[2]);
            
            $sql = $sql.$table_name
                       ." ( `id` bigint(20) UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
                       ."   `hostname` varchar(50) not null, "
                       ."   `type` varchar(30) not null, "
                       ."   `sub_type` varchar(50) not null, ";
            
            $sql_aggregation = $sql_aggregation.$table_name."_summary"
                       ." ( `id` bigint(20) UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
                       ."   `hostname` varchar(50) not null, "
                       ."   `type` varchar(30) not null, "
                       ."   `sub_type` varchar(50) not null, "
                       ."   `create_date` int(8) not null, "
                       ."   `create_time` int(6) not null, "
                       ."   `frequency` varchar(20) not null, ";
                        
            for ($idx = 3; $idx < count($fields); $idx++) {
                if (empty($fields[$idx])) {
                    continue;
                }
                
                $field_name = trim(str_replace("%", "", $fields[$idx]));
                
                if (empty($field_name)) {
                    continue;
                }
                
                $type_str = array_key_exists($field_name, $this->fields_type) ? $this->fields_type[$field_name] : $this->default_type;
                
                $sql = $sql."`".$field_name."` ".$type_str.(!in_array($field_name, $this->key_fields) ? ' null ' : ' not null ');
                
                if ($idx < count($fields)-1) {
                    $sql = $sql.", ";
                }
                
                if ($idx > 6) {
                    if (!in_array($field_name, $this->key_fields)) {
                        if (array_key_exists($field_name, $this->specify_aggregation_fields)) {
                            $sql_aggregation = $sql_aggregation." `".$field_name."` ".$this->fields_type[$field_name]." null, ";
                            
                        } else {
                            for ($idx2 = 0; $idx2 < count($this->aggregation_fields); $idx2++) {
                                $sql_aggregation = $sql_aggregation."`".$field_name.$this->aggregation_fields[$idx2]."` ".$type_str." null ";
                                
                                if ($idx2 < count($this->aggregation_fields)-1 || $idx < count($fields)-1) {
                                    $sql_aggregation = $sql_aggregation.", ";
                                }
                            }
                        }
                        
                    } else {
                        $sql_aggregation = $sql_aggregation."`".$field_name."` ".$type_str." not null";
                        
                        if ($idx < count($fields)-1) {
                            $sql_aggregation = $sql_aggregation.", ";
                        }
                    }
                }
            }
            
            $sql = $sql." );";
            $sql_aggregation = $sql_aggregation." );";
                      
            echo $sql."\n";
            echo $sql_aggregation."\n";
            
            $row_idx++;
        }
        
        fclose($file);
    }
    
    public function createKpiFormulaTable($file_path) {
        $file = fopen($file_path,"r");
        
        $table_map = array();
        $row_idx = 0;
        $ignore_rows = 1;
        while(!feof($file))
        {
            $fields = fgetcsv($file);
            
            if (empty($fields)) {
                continue;
            }
            
            if ($row_idx < $ignore_rows) {
                $row_idx++;
                continue;
            }
            
            $sql = "";
            $table_name = $fields[1];
            $catelog = $fields[2];
            $field_name = $fields[3];
            
            if (strpos($catelog, "#") === 0) {
                //catelog 若有前綴符號(#)表示不需要
                continue;
            }
            
            if (array_key_exists($table_name, $table_map)) {
                $sql = $table_map[$table_name];
                
            } else {
                $sql = $sql." `id` bigint(20) UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
                           ."  `hostname` varchar(50) not null, "
                           ."  `epochtime` int(11) not null, "
                           ."  `localdate` int(11) not null, "
                           ."  `localtime` int(11) not null, "
                           ."  `uptime` int(11) not null, ";
            }
            
            $sql = $sql."`".$field_name."` decimal(15,6) null, ";
            
            $table_map[$table_name] = $sql;
            
            
            $insert_sql = "insert into `kpi_formula_setting` (`table_name`, `catelog`, `kpi_name`, `kpi_formula`, `aggregation_type`) values ( "
                          ."'".$fields[1]."', "
                          ."'".$fields[2]."', "
                          ."'".$fields[3]."', "
                          ."'".$fields[4]."', "
                          ."'".$fields[5]."' );";
            echo $insert_sql."\n";
            
            $row_idx++;
        }
        
        $sql = "";
        foreach ($table_map as $key => $value) {
            $sql = " create table "
                   ." `".$key."` "  //Table name
                   ." ( "
                   .substr($value, 0, strlen($value)-2)          //Table fields
                   ." ); ";
            
            echo $sql."\n";
        }
        
        
        fclose($file);
    }
    
    /**
     * **提供以CSV檔轉換出 ALTER TABLE ___ ADD CONSTRAINT ___ 的SQL
     * @param String $file_path
     */
    public function createConstraints($file_path, $is_summary = false) {
        $file = fopen($file_path,"r");
        
        while(!feof($file))
        {
            $fields = fgetcsv($file);
            
            if (empty($fields)) {
                continue;
            }
            
            $fields = array_values(array_filter($fields));
            if (empty($fields)) {
                continue;
            }
            
            $sql = "alter table `";
            
            $table_name = $this->transTableName($fields[0]);
            
            if ($is_summary) {
                $table_name = $table_name."_summary";
            }
            
            $sql = $sql.$table_name
                       ."` add constraint `"
                       .$table_name
                       ."_uk` unique ( ";
            
            for ($idx = 1; $idx < count($fields); $idx++) {
                $sql = $sql."`".trim($fields[$idx])."`";
                
                if ($idx < count($fields)-1) {
                    $sql = $sql.", ";
                }
            }
            
            $sql = $sql." );";
            echo $sql."\n";
        }
        
        fclose($file);
    }
    
    public function composeSysTableMapping($file_path) {
        $file = fopen($file_path,"r");
        
        $ignore_row_count = 1;
        $row_num = 0;
        while(!feof($file))
        {
            $fields = fgetcsv($file);
            
            if ($row_num < $ignore_row_count) {
                $row_num++;
                continue;
            }
            
            if (empty($fields)) {
                continue;
            }
            
            $table_name = $fields[2];
            for ($idx = 0; $idx < count($fields); $idx++) {
                $sql = "INSERT INTO `sys_table_mapping` (`mapping_id`, `table_name`, `mapping_type`, `ori_name`, `target_name`, `order_num`, `data_type`, `aggregation_type`) VALUES (UUID(), '$table_name', 'FIELD', ";
                
                $field_name = ($idx < 3) ? $this->csv_column_mapping[$idx] : $fields[$idx];
                $field_name = trim(str_replace("%", "", $field_name));
                
                if (empty($field_name)) {
                    continue;
                }
                
                $sql = $sql."'$field_name', '".$field_name."', ".($idx+1);
                
                $data_type = "number";
                if (array_key_exists($field_name, $this->specify_type_fields)) {
                    $data_type = $this->specify_type_fields[$field_name];
                }
                
                $sql = $sql.", '".$data_type."' ";
                
                if ($data_type === "number") {
                    $sql = $sql.", 'AVG');";
                    
                } else if ($data_type === "string") {
                    if (array_key_exists($field_name, $this->specify_aggregation_fields)) {
                        $sql = $sql.", '[VALUE]');";
                        
                    } else {
                        $sql = $sql.", '[KEY]');";
                    }
                }
                
                echo $sql."\n";
            } 
            
            $row_num++;
        }
        
        fclose($file);
    }
    
    private function transTableName($ori_table_name) {
        $str_array = str_split($ori_table_name);
        
        $table_name = "";
        $first_str = true;
        $first_num = true;
        foreach ($str_array as $str) {
            $table_name = $table_name.$this->checkCaseAndTrans($str, $first_str, $first_num);
            
            $first_str = true ? false : false;
            $first_num = $this->checkIsNumeric($str) ? false : true;
        }
        
        return $table_name;
    }
    
    private function checkIsNumeric($str) {
        return preg_match('/^[0-9]+$/', $str);
    }
    
    private function checkCaseAndTrans($str, $first_str = false, $first_num = false) {
        if (preg_match('/^[a-z]+$/', $str)) {
            return $str;
            
        } elseif (preg_match('/^[A-Z]+$/', $str)) {
            if ($first_str) {
                return strtolower($str);
                
            } else {
                return "_".strtolower($str);
            }
            
        } elseif ("-" === $str) {
            return "_";
            
        } elseif ($this->checkIsNumeric($str) && $first_num) {
            return "_".$str;
            
        } else {
            return $str;
        }
    }
}