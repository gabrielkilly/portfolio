#!/usr/local/bin/php
<?php
$servername = "localhost";
$username = "root";
$password = "25Squad";

try {
    $conn = new PDO("mysql:host=$servername;dbname=lucid_books", $username, $password);
    // set the PDO error mode to exception
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    echo "Connected successfully\n";

    $password = "xxxxxxxxxx";
    $university = "Duality-University";
    $y_or_n = ["Y", "N"];
    $table_name  = "users";

    for ($i = 1; $i <= 10; $i++) {

      $full_name = "Person-" . (string)$i;
      $photo_id = "1";
      $email = "person". (string)$i . "somemail.com";
      $accepted = $y_or_n[mt_rand(0, count($y_or_n) - 1)];

      $sql = "INSERT INTO $table_name (full_name, email, password, photo_id, university, accepted)
      VALUES ('$full_name', '$email', '$password', '$photo_id', '$university', '$accepted')";

      $conn->exec($sql);

      echo "Successfully added $full_name to $table_name. \n";
    }
}

catch(PDOException $e)
    {
    echo "Connection failed: " . $e->getMessage() . "\n";
    }
?>
