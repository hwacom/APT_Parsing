<?php
/*
 ** 登入FTP檢核是否有 cDR檔案產生 & 抓回 Parsing server 做後續解析處理動作 
 */

require_once dirname(__FILE__).'/../../vendor/apache/log4php/src/main/php/Logger.php';

require_once dirname(__FILE__).'/../env/Config.inc';

class CDRGetFile
{
    private $logger = null;
    
    private $ftp_ip = null;
    private $ftp_port = null;
    private $ftp_login_account = null;
    private $ftp_login_password = null;
    private $ftp_timeout = null;
    private $ftp_cdr_file_dir = null;
    
    public function __construct() {
        Logger::configure(dirname(__FILE__).'/../env/log4php_cdr_get.xml');
        $this->logger = Logger::getLogger('file');
     
        $this->ftp_ip = CDR_FTP_HOST_ADDRESS;
        $this->ftp_port = CDR_FTP_PORT;
        $this->ftp_login_account = CDR_FTP_USER_NAME;
        $this->ftp_login_password = CDR_FTP_PASSWORD;
        $this->ftp_timeout = CDR_FTP_TIMEOUT;
        $this->ftp_cdr_file_dir = CDR_FTP_FILE_PATH;
    }
    
    
    public function __destruct() {
        unset($GLOBALS['ftp_ip']);
        unset($GLOBALS['ftp_port']);
        unset($GLOBALS['ftp_login_account']);
        unset($GLOBALS['ftp_login_password']);
        unset($GLOBALS['ftp_timeout']);
        unset($GLOBALS['ftp_cdr_file_dir']);
        
        $this->logger = null;
    }
    
    
    public function execute() {
        echo "execute cDR file get !!\n";
        try {
            // set up basic connection
            echo "set up basic connection\n";
            $conn_id = ftp_connect($this->ftp_ip, $this->ftp_port, $this->ftp_timeout);
            echo "conn_id: $conn_id\n";
            
            // login with username and password
            $login_result = ftp_login($conn_id, $this->ftp_login_account, $this->ftp_login_password);
            echo "login_result: $login_result\n";
            
            // check connection
            if ((!$conn_id) || (!$login_result)) {
                echo "FTP connection has failed!";
                echo "Attempted to connect to $this->ftp_ip for user $this->ftp_login_account";
                exit;

            } else {
                echo "Connected to $this->ftp_ip, for user $this->ftp_login_account";
            }
            
            $change_dir = ftp_chdir($conn_id, $this->ftp_cdr_file_dir);
            
        } catch (Exception $t) {
            
        }
    }
}