#!/usr/bin/env perl

use strict;
use warnings;
use JSON::XS;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request::Common qw{ POST };
use Time::Piece;
use DBI;
use Text::CSV_XS qw( csv );

my %stations = (
  S104 => 'Admiralty',
  S109 => 'Ang Mo Kio',
  S24  => 'Changi',
  S121 => 'Choa Chu Kang (South)',
  S50  => 'Clementi',
  S107 => 'East Coast Parkway',
  S44  => 'Jurong (West)',
  S117 => 'Jurong Island',
  S111 => 'Newton',
  S116 => 'Pasir Panjang',
  S06  => 'Paya Lebar',
  S106 => 'Pulau Ubin',
  S80  => 'Sembawang',
  S60  => 'Sentosa Island',
  S43  => 'Tai Seng',
  S115 => 'Tuas South'
);

my $temp_uri = 'http://www.weather.gov.sg/wp-content/themes/wiptheme/page-functions/functions-ajax-temperature-chart.php';
my $dbfile   = './temps.db';

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1, PrintError => 0, AutoCommit => 0});
$dbh->do("CREATE TABLE IF NOT EXISTS TEMPS (dt DATETIME NOT NULL, station_code VARCHAR(5) NOT NULL, temp VARCHAR(6), UNIQUE(dt, station_code))");
$dbh->do("CREATE TABLE IF NOT EXISTS STATIONS(station_code VARCHAR(5) PRIMARY KEY, station_name temp VARCHAR(1255))");
my $stmt = $dbh->prepare("INSERT INTO STATIONS VALUES (:1, :2) ON CONFLICT DO NOTHING");
while (my @station = each(%stations)) {
  $stmt->execute(@station);
}
$dbh->commit;
$dbh->disconnect;

sub populate_temps {
  my($station_code, $station_name) = @_;

  my $ua  = new LWP::UserAgent;
  my $req = POST($temp_uri, [ stationCode => $station_code, hrType => 48]);
  my $content = $ua->request($req)->content;

  my $now = localtime;
  my $offset = -1 * 24 * 60 * 60;
  my $temps = decode_json($content);
  my $firstdate = undef;
  for my $temp (@$temps) {
    unless ($temp->{time} =~ /[ap]m$/) {
      $temp->{time} .= ' '.$now->year;
      $firstdate = $temp->{time} unless ($firstdate);
      $offset += 1 * 24 * 60 * 60;
    } else {
    }
    $temp->{offset} = $offset;
  }
  unless ($firstdate) {
    $firstdate = $now->mday." ".$now->monname;
  }
  for my $temp (@$temps) {
    if ($temp->{time} =~ /[ap]m$/) {
      $temp->{datetime} = (Time::Piece->strptime("$firstdate $temp->{time}", "%d %b %Y %X") + $temp->{offset})->datetime;
    } else {
      $temp->{datetime} = (Time::Piece->strptime("$firstdate", "%d %b %Y") + $temp->{offset})->datetime;
    }
  }

  #print Dumper($temps);

  my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1, PrintError => 0, AutoCommit => 0});
  my $stmt = $dbh->prepare("INSERT INTO TEMPS VALUES (:1, :2, :3) ON CONFLICT(dt, station_code) DO UPDATE SET temp = :3");
  for my $temp (@$temps) {
    eval {
      $stmt->execute($temp->{datetime}, $station_code, $temp->{temp});
      #print "INSERTED $temp->{datetime}, $temp->{temp}\n";
    };
    if ($@) {
      die($@) unless $@ =~ /UNIQUE constraint failed/;
    }
  }
  $dbh->commit;
  $dbh->disconnect;
}

while (my @station = each(%stations)) {
  populate_temps @station;
}

$dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1, PrintError => 0});
$stmt = $dbh->prepare("SELECT date as 'Datetime', station_name as 'Station Name', T.station_code as 'Station Code', MAX(temp) as 'Max Temp', MIN(temp) as 'Min Temp', AVG(temp) as 'Avg Temp' \
  FROM (SELECT temp, station_code, STRFTIME('%Y%m%d', DATETIME(dt, '-3 hours')) AS date \
        FROM TEMPS WHERE temp IS NOT NULL) T JOIN STATIONS S ON T.station_code = S.station_code
  GROUP BY date, T.station_code");
my $rv = $stmt->execute;
#print Dumper($stmt->{NAME});
#print Dumper($stmt->fetchall_arrayref);
#csv (in => sub { $stmt->fetchrow_arrayref }, out => "temps.csv", headers => $stmt->{NAME_lc}) or die Text::CSV_XS->error_diag;
my $headers_out = undef;
csv (in  => sub { if ($headers_out) { $stmt->fetchrow_arrayref } else { $headers_out = 1; $stmt->{NAME} } },
     out => "temps.csv",
     headers => "lc") or die Text::CSV_XS->error_diag;

$dbh->disconnect;

system('aws s3 mv temps.csv s3://YOUR_OWN_BUCKET_NAME/temps.csv');

# vim: ts=2 sw=2 et number si
