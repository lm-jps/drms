#!/usr/bin/perl -w 

# Runs the cptest program with various sets of arguments and checks the results returned by the program
# XXX This is taking too long - finish later.

my(@named); # array of hash references of named arguments
my(@namedmm);
my(@unnamedmm);
my(@flag);
my(@gflag);
my(@iarray);
my(@farray);
my(@darray);
my(@nume);

my(@anarr);
my(@anotherarr);
my($arrref);

my(%emptyhash); # use this if you want to omit one or more types of args from the cmd line.

# Arguments - to determine the cmd-line, use one element from each of the arrays of hashes. Each of 
# these elements has the same array index.

# named arguments
@anarr = qw(a 3 b 19 strarg1 fred intarg 26 dblarg 23.89);
@anotherarr = ("strarg2", "fred is cool"); # qw doesn't handle spaces
@anarr = (@anarr, @anotherarr);
$arrref = makehashref(@anarr);
push(@named, $arrref);


# unnamed arguments
@anarr = qw(alpha beta delta);
$arrref = makehashrefUnary("u", @anarr);
push(@unnamed, $arrref);


# --named arguments
@anarr = qw(george 10.3);
$arrref = makehashref(@anarr);
push(@namedmm, $arrref);


# --unnamed arguments
@anarr = qw(nonameflag);
$arrref = makehashrefUnary("u", @anarr);
push(@unnamedmm, $arrref);


# single flags
@anarr = qw(g i o);
$arrref = makehashrefUnary("1", @anarr);
push(@flags, $arrref);


# grouped flags
@anarr = qw(EatlM bO);
$arrref = makehashrefUnary("1", @anarr);
push(@gflags, $arrref);


# arrays (ints, floats, doubles)
@anarr = qw(intsarg [2,3,6,7,2]);
$arrref = makehashref(@anarr);
push(@iarray, $arrref);

@anarr = qw(fltsarg [2.2353,73.234]);
$arrref = makehashref(@anarr);
push(@farray, $arrref);

@anarr = qw(dblsarg [2.23523,3.23525235,6.235232,7.23523523,2]);
$arrref = makehashref(@anarr);
push(@darray, $arrref);


# enumerations
@anarr = qw(numearg violet);
$arrref = makehashref(@anarr);
push(@nume, $arrref);

# Run cmd-line
my($ihash) = 0;

my($namedset);
my($unnamedset);
my($namedmmset);
my($unnamedmmset);
my($flagset);
my($gflagset);
my($iarrayset);
my($farrayset);
my($darrayset);
my($numeset);

my($cmd) = "";
my($iarg) = 0;

while ($ihash < scalar(@named))
{
   $namedset = $named[$ihash];
   $unnamedset = $unnamed[$ihash];
   $namedmmset = $namedmm[$ihash];
   $unnamedmmset = $unnamedmm[$ihash];
   $flagset = $flag[$ihash];
   $gflagset = $gflag[$ihash];
   $iarrayset = $iarray[$ihash];
   $farrayset = $farray[$ihash];
   $darrayset = $darray[$ihash];
   $numeset = $nume[$ihash];

   while(my($k, $v) = each (%$namedset))
   {
      if ($iarg > 0)
      {
         $cmd = join("", $cmd, " ");
      }

      $cmd = $cmd . $k . "=" . "\"$v\"";
      $iarg++;
   }

   print STDOUT "$cmd.\n";
   $ihash++;
}


# test expected results


sub makehashref
{
   my(@inarr) = @_;
   my($hashref);
   my($iarr);

   $iarr = 0;
   while ($iarr < scalar(@inarr))
   {
      $key = $inarr[$iarr];
      $iarr++;
      $value = $inarr[$iarr];
      $iarr++;

      $hashref->{$key} = $value;      
   }

   return $hashref;
}

sub makehashrefUnary
{
   my($val) = shift(@_);
   my(@inarr) = @_;
   my($hashref);
   my($iarr);

   $iarr = 0;
   while ($iarr < scalar(@inarr))
   {
      $key = $inarr[$iarr];
      $iarr++;
      $value = $val;

      $hashref->{$key} = $value;      
   }

   return $hashref;
}
