<?php

$servername = "localhost";
$username = "root";
$password = "25Squad";
$dbname = "lucid_books";

try {
    $conn = new PDO("mysql:host=$servername;dbname=$dbname", $username, $password);
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $stmt = $conn->prepare("SELECT id, full_name, email, password, photo_id, university, accepted FROM users WHERE accepted = 'Y'");
    $stmt->execute();
    $photo_id = "default.jpg";
    // set the resulting array to associative
    $result = $stmt->fetchAll();
  ?>

<?php
    for ($i = 0; $i < count($result); $i++)  {
?>
  <style>
    .btn{
      height: 30px;
      width: 80px;
    }
  </style>
  <div style="height: 300px; width: 700px; border:2px black solid; margin: auto; display: inline: block; margin-top: 30px;">
    <ul style="display: block; margin: auto;">
      <li style="display: inline-block; position: relative; top: 40px;" >
        <div style="height: 200px; width: 200px; background-color: red;"></div>
      </li>
      <li style="display: inline-block; position: relative; margin-left: 30px;" >
        <div style="height: 200px; width: 160px;">
          <h2 ><?= $result[$i]["full_name"]; ?></h2>
          <h3><?= $result[$i]["university"]; ?></h3>
          <button class = "btn"></button>
          <button class = "btn"></button>
        </div>
      </li>

    </ul>
  </div>
<?php
    }
}
catch(PDOException $e) {
    echo "Error: " . $e->getMessage();
}
$conn = null;

?>
