<?php
/*
 ** �w��ѪR������cDR output�ɮסAŪ�����e & �g�JDB
 */
require_once dirname(__FILE__).'/../../vendor/apache/log4php/src/main/php/Logger.php';

require_once dirname(__FILE__).'/../env/Config.inc';

class CDRParsing
{
    private $logger = null;
    
    public function __construct() {
        Logger::configure(dirname(__FILE__).'/../env/log4php_cdr_parsing.xml');
        $this->logger = Logger::getLogger('file');
        
    }
    
    
    public function __destruct() {
        
        $this->logger = null;
    }
    
    
    public function execute() {
        try {
            
            
        } catch (Exception $t) {
            
        }
    }
}