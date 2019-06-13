<?php
namespace Hwacom\APT_Parsing\dao;

use Exception;

class DatabaseAccessObject {
    private $mysql_address = "";
    private $mysql_username = "";
    private $mysql_password = "";
    private $mysql_database = "";
    private $link;
    private $last_sql = "";
    private $last_id = 0;
    private $last_num_rows = 0;
    private $error_message = "";
    
    /**
     ** �o�q�O�y�غc���z�|�b����Q new �ɦ۰ʰ���A�̭��D�n�O�إ߸��Ʈw���s���A�ó]�w�y�t�O�U��y���H�䴩����
     */
    public function __construct($mysql_address, $mysql_username, $mysql_password, $mysql_database) {
        $this->mysql_address  = $mysql_address;
        $this->mysql_username = $mysql_username;
        $this->mysql_password = $mysql_password;
        $this->mysql_database = $mysql_database;
        
        $this->link = ($GLOBALS["___mysqli_ston"] = mysqli_connect($this->mysql_address, $this->mysql_username, $this->mysql_password));
        
        if (mysqli_connect_errno())
        {
            $this->error_message = "Failed to connect to MySQL: " . mysqli_connect_error();
            echo $this->error_message;
            return false;
        }
        mysqli_query($GLOBALS["___mysqli_ston"], "SET NAMES utf8");
        mysqli_query($this->link, "SET NAMES utf8");
        mysqli_query($this->link, "SET CHARACTER_SET_database= utf8");
        mysqli_query($this->link, "SET CHARACTER_SET_CLIENT= utf8");
        mysqli_query($this->link, "SET CHARACTER_SET_RESULTS= utf8");
        
        if(!(bool)mysqli_query($this->link, "USE ".$this->mysql_database))$this->error_message = 'Database '.$this->mysql_database.' does not exist!';
    }
    
    /**
     ** �o�q�O�y�Ѻc���z�|�b����Q unset �ɦ۰ʰ���A�̭�������O�O���_���Ʈw���s��
     */
    public function __destruct() {
        mysqli_close($this->link);
    }
    
    /**
     ** �o�q�ΨӰ��� MYSQL ��Ʈw���y�k�A�i�H�F���ϥ�
     */
    public function execute($sql = null) {
        if ($sql===null) return false;
        $this->last_sql = str_ireplace("DROP","",$sql);
        $result_set = array();
        
        $result = mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            $this->error_message = "MySQL ERROR: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
        } else {
            $this->last_num_rows = @mysqli_num_rows($result);
            for ($xx = 0; $xx < @mysqli_num_rows($result); $xx++) {
                $result_set[$xx] = mysqli_fetch_assoc($result);
            }
            if(isset($result_set)) {
                return $result_set;
            }else{
                $this->error_message = "result: zero";
            }
        }
    }
    
    /**
     ** �o�q�Ψ�Ū����Ʈw������ơA�^�Ǫ��O�}�C���
     */
    public function query($table = null, $condition = "1", $order_by = "1", $fields = "*", $limit = ""){
        $sql = "SELECT {$fields} FROM {$table} WHERE {$condition} ORDER BY {$order_by} {$limit}";
        echo "SQL: $sql\n";
        return $this->execute($sql);
    }
    
    public function truncateTable($sql, $table) {
        $stmt = mysqli_stmt_init($this->link);
        
        echo "truncate sql: $sql , table: $table\n";
        if (mysqli_stmt_prepare($stmt, $sql)) {
            mysqli_stmt_bind_param($stmt, "s", $table);
            mysqli_stmt_execute($stmt);
            echo "truncate execute\n";
            mysqli_stmt_close($stmt);
        }
    }
    
    public function executeSQL($sql) {
        $this->last_sql = $sql;
        
        mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            $error_msg = "MySQL Update Error: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
            throw new \Exception($error_msg);
        }
    }
    
    public function insertBySQL($sql) {
        $this->last_sql = $sql;
        
        mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            $error_msg = "MySQL Update Error: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
            throw new \Exception($error_msg);
            
        } else {
            $this->last_id = mysqli_insert_id($this->link);
            return $this->last_id;
        }
    }
    
    /**
     ** �o�q�i�H�s�W��Ʈw������ơA�ç�̫�@���� ID �s���ܼƤ��A�i�H�� getLastId() ���X
     */
    public function insert($table = null, $data_array = array()) {
        if($table===null)return false;
        if(count($data_array) == 0) return false;
        
        $tmp_col = array();
        $tmp_dat = array();
        
        foreach ($data_array as $key => $value) {
            try {
                $value = mysqli_real_escape_string($this->link, $value);
                
            } catch (Exception $e) {
                $this->logger->error( "Caught exception:  ".$e->getMessage() );
            }
            
            //$value = htmlspecialchars($value, ENT_QUOTES, "ISO-8859-1");
            //$value = filter_var($value, FILTER_SANITIZE_STRING);
            //$value = $this->link->real_escape_string($value);
            
            $tmp_col[] = '`'.$key.'`';
            $tmp_dat[] = "'$value'";
        }
        
        $columns = join(",", $tmp_col);
        $data = join(",", $tmp_dat);
        
        $this->last_sql = "INSERT INTO `" . $table . "` (" . $columns . ") VALUES (" . $data . ")";
        
        //echo "$this->last_sql;\n";
        
        mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            $error_msg = "MySQL Update Error: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
            throw new \Exception($error_msg);
            
        } else {
            $this->last_id = mysqli_insert_id($this->link);
            return $this->last_id;
        }
        
    }
    
    /**
     ** �o�q�i�H��s��Ʈw�������
     */
    public function update($table = null, $data_array = null, $key_column = null, $id = null) {
        if($table == null){
            echo "table is null";
            return false;
        }
        if($id == null) return false;
        if($key_column == null) return false;
        if(count($data_array) == 0) return false;
        
        $id = mysqli_real_escape_string($this->link, $id);
        
        $setting_list = "";
        $count = count($data_array);
        for ($xx = 0; $xx < $count; $xx++) {
            list($key, $value) = each($data_array);
            $value = mysqli_real_escape_string($this->link, $value);
            $setting_list .= $key . "=" . "\"" . $value . "\"";
            if ($xx != $count - 1)
                $setting_list .= ",";
        }
        $this->last_sql = "UPDATE " . $table . " SET " . $setting_list . " WHERE " . $key_column . " = " . "\"" . $id . "\"";
        $result = mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            echo "MySQL Update Error: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
        } else {
            return $result;
        }
    }
    
    public function updateByUK($table = null, $data_array = null, $uk_array = null) {
        if($table == null){
            echo "table is null";
            return false;
        }
        if(count($data_array) == 0) return false;
        if(count($uk_array) == 0) return false;
        
        $id = mysqli_real_escape_string($this->link, $id);
        
        $setting_list = "";
        $count = count($data_array);
        for ($xx = 0; $xx < $count; $xx++) {
            list($key, $value) = each($data_array);
            $value = mysqli_real_escape_string($this->link, $value);
            $setting_list .= $key . "=" . "\"" . $value . "\"";
            if ($xx != $count - 1)
                $setting_list .= ",";
        }
        
        $uk_list = "";
        $count = count($uk_array);
        for ($xx = 0; $xx < $count; $xx++) {
            list($key, $value) = each($uk_array);
            $value = mysqli_real_escape_string($this->link, $value);
            $uk_list .= $key . "=" . "\"" . $value . "\"";
            if ($xx != $count - 1)
                $uk_list .= ",";
        }
        
        $this->last_sql = "UPDATE " . $table . " SET " . $setting_list . " WHERE " . $uk_list;
        $result = mysqli_query($this->link, $this->last_sql);
        
        if (((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false))) {
            echo "MySQL Update Error: " . ((is_object($this->link)) ? mysqli_error($this->link) : (($___mysqli_res = mysqli_connect_error()) ? $___mysqli_res : false));
        } else {
            return $result;
        }
    }
    
    /**
     ** �o�q�i�H�R����Ʈw�������
     */
    public function delete($table = null, $key_column = null, $id = null) {
        if ($table===null) return false;
        if($id===null) return false;
        if($key_column===null) return false;
        
        return $this->execute("DELETE FROM $table WHERE " . $key_column . " = " . "\"" . $id . "\"");
    }
    
    public function deleteByUK($table = null, $uk_array = null) {
        if ($table===null) return false;
        if($uk_array===null) return false;
        
        $uk_list = "";
        $count = count($uk_array);
        for ($xx = 0; $xx < $count; $xx++) {
            list($key, $value) = each($uk_array);
            $value = mysqli_real_escape_string($this->link, $value);
            $uk_list .= $key . "=" . "\"" . $value . "\"";
            if ($xx != $count - 1)
                $uk_list .= ",";
        }
        
        return $this->execute("DELETE FROM $table WHERE " . $uk_list);
    }
    
    /**
     ** @return string
     ** �o�q�|��̫���檺�y�k�^�ǵ��A
     */
    public function getLastSql() {
        return $this->last_sql;
    }
    
    /**
     ** @param string $last_sql
     ** �o�q�O����檺�y�k�s���ܼƸ̡A�]�w�� private �u�������i�H�ϥΡA�~���L�k�I�s
     */
    private function setLastSql($last_sql) {
        $this->last_sql = $last_sql;
    }
    
    /**
     ** @return int
     ** �D�n�\��O��s�W�� ID �Ǩ쪫��~��
     */
    public function getLastId() {
        return $this->last_id;
    }
    
    /**
     ** @param int $last_id
     ** ��o�� $last_id �s�쪫�󤺪��ܼ�
     */
    private function setLastId($last_id) {
        $this->last_id = $last_id;
    }
    
    /**
     ** @return int
     */
    public function getLastNumRows() {
        return $this->last_num_rows;
    }
    
    /**
     ** @param int $last_num_rows
     */
    private function setLastNumRows($last_num_rows) {
        $this->last_num_rows = $last_num_rows;
    }
    
    /**
     ** @return string
     ** ���X���󤺪����~�T��
     */
    public function getErrorMessage()
    {
        return $this->error_message;
    }
    
    /**
     ** @param string $error_message
     ** �O�U���~�T���쪫���ܼƤ�
     */
    private function setErrorMessage($error_message)
    {
        $this->error_message = $error_message;
    }
}