#!/home/jsoc/bin/linux_x86_64/perl5.12.2 -w

use JSON -support_by_pp;

unless ($#ARGV == 0) { die "Usage: extractexpsize.pl <path to index.json>\n"; }

my($file) = $ARGV[0];
my($json) = JSON->new->utf8;
my(@contentarr);
my($content);
my($txt);
my($size) = -1;

if (open(JSONCONTENT, "<$file"))
{
   @contentarr = <JSONCONTENT>;
   $content = join('', @contentarr);
   $txt = $json->decode($content);
   
   # Extract the size property.
   $size = $txt->{size};

   close(JSONCONTENT);
}
else
{
   die "Unable to open $file.\n";
}

print "$size\n";
exit(0);
