<?php
$servername = "localhost";
$username = "root";
$password = "25Squad";

try {
  $conn = new PDO("mysql:host=$servername;dbname=lucid_books", $username, $password);
  // set the PDO error mode to exception
  $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  echo "Connected successfully\n";

  $table_name  = "users";

  $sql = "DELETE FROM $table_name WHERE full_name like 'Person-%'";
  $conn->exec($sql);

  echo "Test Users Successfully Deleted. \n";
}

catch(PDOException $e){
  echo "Connection failed: " . $e->getMessage() . "\n";
}
?>
