#!perl --

# generate config scripts rather than follow the instructions
# and have to look at all of variables to change around,
# when most are in the 'config.local' file

# 2009/04/16 : jhourcle : 1.0 : first version released
# 2009/04/17 : jhourcle : 1.1 : don't do anything without confirmation; 
#                               deal with missing config AND template

# LIMITATIONS :
# assumes UNIX directory path seperators (/)
# currently, only tested under macosx to automatically determine partition sizes
# could hose an existing install, as I reset passwords, etc.
#  -- probably need an 'install' and an 'update'

use strict;
use warnings;
use Data::Dumper;

####
# before we go any further ...
warn <<"EOF";

    This will create and/or overwrite your config.local file, and will
    generate scripts to perform the rest of a DRMS / SUMS install.

    Please review all files before running them for unescaped characters.

EOF
exit if ( my $flag = prompt ('Continue?', 'yes') ) !~ m/^[yY]/;
####

#####
# configuration ( for the script )

# where we expect the file to be
my $config = 'config.local';

# where we dump the generated scripts to
my $output_dir = 'setup_scripts';

# if true, don't bother prompting for changes to
# the config file
my $noprompt = 0;

# verbosity level
my $verbose = 0;

# the fields we need in the config script.
# (I _could_ read this from config.local.template, but I don't
# know if that's always safe)

my @fields = qw (
	LOCAL_CONFIG_SET
	DRMS_DATABASE
	DBSERVER_HOST
	DRMS_SITE_CODE
	POSTGRES_ADMIN
	POSTGRES_INCS
	POSTGRES_LIBS
	SUMS_MANAGER
	SUMS_SERVER_HOST
	SUMS_LOG_BASEDIR
	THIRD_PARTY_LIBS
	THIRD_PARTY_INCS
	DRMS_SAMPLE_NAMESPACE

	POSTGRES_DATA_DIR
	SUMS_PARTITIONS
	SUMS_FILL_TO_LINE
	SUMS_GROUP
	SUMS_USER
	DRMS

);


#####
# Check what options were passed in

use Getopt::Long; 
GetOptions(
	'config=s' => \$config,
	'output=s' => \$output_dir,
	noprompt => \$noprompt,
	'verbose:+' => \$verbose
);

#####
# setup other variables

my $EOL = $verbose > 2 ? '' : "\n" ;

use ExtUtils::MakeMaker;
if ($noprompt) { $ENV{'PERL_MM_USE_DEFAULT'} = 1 };

if ($verbose > 2) { print <<EOF; }
 
verbosity : $verbose
noprompt? : $noprompt
config    : $config
out dir   : $output_dir

EOF

####
# TODO : prompt to create or update the config file

# read the config file
# this might seem strange -- we'll copy the template to make the config file,
# but we want to find the config file to look to see where the template might
# be (so we can pull in the descriptions)

my ($config_lines, %config);

if ( defined($config) and -f $config) {
	($config_lines, %config) = read_config($config);
}

sub read_config {
	my $config = shift;
	print "Reading config file ($config) ...\n" if $verbose;

	open (CONFIG, '<', $config )
		or die "Can't read from config file ($config) : $!$EOL";
	my @config = <CONFIG>;
	my %config = map {
		my ($key,$val) = m/(\S+)\s+(.*)/;
		if ( defined($val) ) {
			($key,$val);
		} else {
			warn "unparsed line : $_$EOL";
			();
		}
	} grep { m/\S/ } grep { ! m/^#/ } @config;
	close (CONFIG);
	return(\@config, %config);
}

# check on the template file

warn "Checking template ... $EOL" if $verbose;

my $template;
my @templates = qw( ./config.local.template );
if ( defined($config)       ) { unshift @templates, "$config.template" }
if ( defined($config{DRMS}) ) { unshift @templates, "$config{DRMS}/config.local.template" }
foreach my $test  ( @templates ) {
	if ( -f $test ) { $template = $test; last }
}

my @template_file = <DATA>;
if ( ! -f $config and defined($template) ) { 
	print "\nUsing template to generate config file\n\n";
	# no config file
	use File::Copy;
	if ( ! copy( $template, $config ) ) {
		warn "Can't copy template : $!$EOL";
		if ( ! @$config_lines ) {
			die "No config file, and can't copy template.  Aborting.$EOL";
		}
	}
	($config_lines, %config) = read_config($config);
} elsif ( ! -f $config ) {
	# use the contents of the __DATA__ section as the template
	warn "\nNo template file!  Regenerating from script.\n $EOL";
	if ( open ( CONFIG, '>', $config ) ) {
		print CONFIG @template_file;
		close CONFIG
			or warn "Can't save config ($config) : $!$EOL";	
		($config_lines, %config) = read_config($config);
	} else {
		warn "Can't write config ($config) :$!$EOL";
	}
}

# read the template file for its comments.

warn "Reading template ($template) ... $EOL" if $verbose;

# you _could_ set some values here, but take a look in the __DATA__ block
# first
my %descriptions = ();
my %template_defaults = ();

if ( defined($template) and -f $template ) {
	open (TEMPLATE, '<', $template)
		or warn "Can't read template ($template) : $!$EOL"
		and last;
	push ( @template_file, <TEMPLATE> );
	close TEMPLATE;
}

my $description = '';
while ( @template_file ) {
	my $line = shift @template_file;

	if ($line =~ m/^\s*$/) { # empty line
		$description = '';
		next;
	}
	if ($line =~ m/^\s*#/) { # starts with '#'
		$description .= $line;
		next;
	}
	if ( $line =~ m/(\S+)\s+(.*)/ ) { # something got set
		my ($key,$val) = ($1,$2);
		chomp $val;
		$descriptions{$key} = $description;
		$template_defaults{$key} = $val;
		$description = '';
	}
}
		

#####


warn "Verifying config ...\n" if $verbose;

my $config_changed = 0;
while ( 1 ) {

	# prompt to update / change values
	print "Current configuration settings:\n",
		map { sprintf "    %32s : %s\n", $_, (defined($config{$_})?$config{$_}:'(undefined)') } @fields;

	my $response = prompt( "Use current settings?", 'yes' );
	last if $response =~ m/^[yY]/; # exit current (CHANGECONFIG) block

	warn "setting changed flag ...$EOL" if $verbose;
	$config_changed = 1;

	foreach my $field (@fields) {
		print $descriptions{$field};
		$config{$field} = prompt( $field, $config{$field} );
	}
}

warn "Config changed status : $config_changed $EOL" if $verbose;

if ($config_changed) {
	# we try to use the existing order of the config file, but add in
	# new values (w/ descriptions) at the end.

	# note -- this will remove 'comment' blocks in the template
	# that aren't directly before a variable.

	warn "Generating new config file ... $EOL" if $verbose;

	my @new_config;
	my %unprocessed = map { ($_ => undef) } @fields;


	while ( @$config_lines ) {
		my $line = pop @$config_lines;
		if ( $line =~ m/^\s*(#|$)/ ) {
			unshift (@new_config, $line);
       		} elsif ( $line =~  m/(\S+)\s+(.*)/ ) {
			my $field = $1;
			unshift (@new_config, "$field\t$config{$field}\n");
			delete($unprocessed{$field});
		} else {
			unshift (@new_config, $line);
		}
	}

	warn "Unprocessed fields : @{[ keys %unprocessed ]} $EOL" if $verbose > 2;

	warn "Checking new items ... $EOL" if $verbose;
	if ( scalar %unprocessed ) {
		foreach my $field (@fields) {
			next if !exists($unprocessed{$field});
			push (@new_config, "\n", $descriptions{$field}, "$field\t$config{$field}\n");
		}
	}

	warn "Writing config file ($config) ... $EOL" if $verbose;
	if ( open (CONFIG, '>', $config) ) {
		warn "Config file contents :", Dumper(\@new_config) if $verbose > 2;
		print CONFIG @new_config;
		close (CONFIG)
			or warn "Can't save to config file ($config) : $!\nConfig changes not saved\n  $EOL";
	} else {
		warn "Can't write to config file ($config) : $!\nConfig changes not saved\n  $EOL";
	}
	warn "... done $EOL" if $verbose;
}


# check to make sure we got all of the fields we care about.

print "Checking config values ...\n" if $verbose;

my $missing = 0;
foreach my $field (@fields) {
	if (!defined($config{$field})) {
		$missing++;
		warn "Config file is missing value for : $field$EOL";
	}
}
die "Aborting$EOL" if $missing;


#####
# prep directory to output to

if ( ! -d $output_dir ) {
	if ( ! -e $output_dir ) {
		mkdir ($output_dir) or
			die "Can't create directory ($output_dir) : $!$EOL";
	} else {
		die "Output directory exists but isn't a directory ($output_dir) $EOL";
	}
}

#### 
# now we write some files

write_file( 'README', <<"EOF" );

# Configuration scripts for NetDRMS generated at
#  @{[ scalar localtime() ]}

The following scripts should be generated, that you will need
to run:

    01_create_databases.sh
        To be run as the database user ($config{POSTGRES_ADMIN})
	Will generate the postgres databases for DRMS and SUMS
        and call the sql scripts to create the necessary tables

    03_create_sums_partitions.pl
	Perl script to make the SUMS partitions and set their
        ownership.  Should be run as root or via sudo.
	(but you should look at it first, and make sure there's no
	unescaped characters)

    start_sums
	This is called as part of the '01_create_databases.sh'
	script, but it's there as a convenience for calling after
	a reboot or whatever.

Files generated, but you should never need to call directly:

    README
	(this file)

    02_create_drms_accounts.sql
    02_create_sums_accounts.sql
	SQL scripts to add the SUMS user ($config{SUMS_MANAGER})
	to the database.  These are called as part of
	01_create_databases.sh

	WARNING : THESE HAVE DATABASE PASSWORDS IN THEM.

	(or at least, they will, after you've run the script, as
	technically, the passwords aren't in use yet)

    04_create_sums_partitions.sql
	SQL script to let SUMS know about the partitions it should
	be using.  (called by 01_create_databases.sh )
		

EOF


write_file( '01_create_databases.sh', <<"EOF" );
#!sh --

# This file should be run as the database user ($config{POSTGRES_ADMIN})
# from within the directory it's in (so it can find the sql scripts)

initdb --local=C -D '$config{POSTGRES_DATA_DIR}'
initdb --local=C -D '$config{POSTGRES_DATA_DIR}_sums'

perl -i.orig -e 's/#port = 5432/port = 5434/' '$config{POSTGRES_DATA_DIR}_sums'

pg_ctl start -D '$config{POSTGRES_DATA_DIR}'      -l '$config{POSTGRES_DATA_DIR}/logfile'
pg_ctl start -D '$config{POSTGRES_DATA_DIR}_sums' -l '$config{POSTGRES_DATA_DIR}_sums/logfile'

createdb -E LATIN1 '$config{DRMS_DATABASE}'
createdb -E LATIN1 '$config{DRMS_DATABASE}_sums'

createlang plpgsql '$config{DRMS_DATABASE}'

psql         -f $config{DRMS}/scripts/NetDRMS.sql            -d '$config{DRMS_DATABASE}'
psql         -f ./02_create_drms_accounts.sql     -d '$config{DRMS_DATABASE}'
psql         -f $config{DRMS}/scripts/drms_series.sql        -d '$config{DRMS_DATABASE}'
psql         -f $config{DRMS}/scripts/drms_session.sql       -d '$config{DRMS_DATABASE}'


psql -p 5434 -f $config{DRMS}/scripts/create_sums_tables.sql -d '$config{DRMS_DATABASE}_sums'
psql -p 5434 -f $config{DRMS}/scripts/create_sumindex.sql    -d '$config{DRMS_DATABASE}_sums'
psql -p 5434 -f ./02_create_sums_accounts.sql     -d '$config{DRMS_DATABASE}_sums'

psql -p 5434 -f ./04_create_sums_partitions.sql     -d '$config{DRMS_DATABASE}_sums'

make

# must be done after everything is built ?
masterlists dbuser='$config{SUMS_MANAGER}' namespace=drms

make sums

./start_sums

EOF

###

# this value really shouldn't be in the config ... but we need to deal with it.
if (! defined($config{SUMS_MANAGER_PASSWORD}) ) {
	if ($noprompt) {
		$config{SUMS_MANAGER_PASSWORD} = 'ChangeMe'.int(rand(999999));
		print "\n\nAssigning password for database user '$config{SUMS_MANAGER}' to '$config{SUMS_MANAGER_PASSWORD}'\n\n";
	} else {
		# no hitting return just to get by it.
		until ( defined( $config{SUMS_MANAGER_PASSWORD}) and ($config{SUMS_MANAGER_PASSWORD} =~ m/\S/) ) {
			$config{SUMS_MANAGER_PASSWORD} = prompt( 'Enter a password for the SUMS manager: ' );
		}
	}
}
$config{SUMS_MANAGER_PASSWORD} =~ s/'/''/g;

		
write_file( '02_create_drms_accounts.sql', <<"EOF" );
-- this should be called from 01_....sh
create role jsoc;
create role sumsadmin;
create user $config{SUMS_MANAGER};
alter user $config{SUMS_MANAGER} with password '$config{SUMS_MANAGER_PASSWORD}'
EOF

write_file( '02_create_sums_accounts.sql', <<"EOF" );
-- this should be called from 01_...sh
create user $config{SUMS_MANAGER};
alter user $config{SUMS_MANAGER} with password '$config{SUMS_MANAGER_PASSWORD}'
grant all on sum_tape to $config{SUMS_MANAGER};
grant all on sum_ds_index_seq,sum_seq to $config{SUMS_MANAGER};
grant all on sum_file,sum_group,sum_main,sum_open to $config{SUMS_MANAGER};
grant all on sum_partn_alloc,sum_partn_avail to $config{SUMS_MANAGER};
EOF



###

my @partitions = split( /\s+/, $config{'SUMS_PARTITIONS'} );

my $fill_to_line = $config{'SUMS_FILL_TO_LINE'};

my %sizes = map {( $_, int(get_partition_size($_)*$fill_to_line) )} @partitions;

warn ("Partition sizes : ", Dumper(\%sizes) ) if $verbose > 1;

write_file( '03_create_sums_partitions.pl',
	"#!perl --\n\n# make sure partitions for SUMS are there\n# run as root (or use sudo)\n\n",
	map { <<"EOF" } @partitions );
mkdir -p '$_'
chmod 2770 '$_'
chown '$config{SUMS_MANAGER}' '$_'
chgrp '$config{SUMS_GROUP}' '$_'

EOF

write_file( '04_create_sums_partitions.sql', "-- run this on the sums database\n\n", map { <<"EOF" } @partitions );
insert into sum_partn_avail (partn_name,total_bytes,avail_bytes,pds_set_num) values ($_, $sizes{$_}, $sizes{$_}, 0);
EOF

###

my $PLATFORM = `$config{DRMS}/build/jsoc_machine.csh`;
chomp $PLATFORM;
# use Cwd;
# my $cwd = cwd();
write_file ( 'start_sums', <<"EOF" );
#!sh --

'$config{DRMS}/bin/$PLATFORM/sum_svc '$config{DRMS_DATABASE}_sums' &
EOF


###

print "\n\nFiles generated.  Please read $output_dir/README for instructions\n\n";


#############
# subroutines 

# someone's going to bitch that '$output_dir' and '$EOL' are globals

use Carp qw( croak );

sub write_file {
	my $filename = "$output_dir/".shift; 
	warn "Writing file ... $filename $EOL" if $verbose;
	open (OUTPUT, '>', $filename )
		or croak "Can't write file ($filename) $!$EOL";
	print OUTPUT @_;
	close (OUTPUT)
		or croak "Can't save file ($filename) $!$EOL";
	return 1;
}

# we'll probably need to adjust this for other OSes
sub get_partition_size {
	my $dir = shift;
	if ( $dir !~ m#/$# ) { $dir .= '/' }	
	if ( ! -d $dir ) {
		if ( $dir =~ m#(.*/)[^/]*/$# ) { 
			@_ = ( $1, $dir );
			goto &get_partition_size;
		} 
		$dir = shift || $dir;
		warn "No such directory for SUMS partition : $dir\nPlease create the directory\n  $EOL";
		return 0;
	}
 	my $info = `df -k '$dir'`;
	if ($info) {
		my $line = [ split("\n", $info) ]->[-1];
		my ($partition, $max, $used, $free, undef) = split( /\s+/, $line );
		# might have to keep track of already seen partitions, to keep from screwing up
	
		if ( defined($free) ) {	
			return $free*1024;
		} else {
			# parsing failed
		}
	}
	# command failed
	return 0;
}

__DATA__
# NetDRMS local site configuration info
# edit the values in the second column of each line in this file to reflect
#   the values appropriate to your site

# a marker to indicate whether this file has been checked/edited. You MUST
# either change its value to yes (or anything but NO) or comment it out.
LOCAL_CONFIG_SET        YES

# the next three entries must almost certainly be changed to reflect your
#   local configuration

# the name of the NetDRMS database; the SUMS database will be assumed to
#   have the same name with "_sums" appended
# see http://vso.stanford.edu/netdrms/site_codes.html
DRMS_DATABASE   mydb

# the host name of the default database server you will be accessing; you
#   should include the internet domain (e.g. host.subnet.net) if the server is
#   not on your subnet; but if it is on your subnet it may be better not to
# the default value is only really appropriate if you are running in a
#   single-user environment, such as a laptop; whether it is the default
#   or a named host depends on how the postgres database named above and
#   its dependent _sums have been configured in their pg_hba.conf files:
#   localhost for METHOD "trust", a named host for METHOD "ident sameuser"
DBSERVER_HOST   localhost

# a 15-bit numerical site identifier; values < 16384 (0x4000) are for
#   publicly exporting sites, and must be registered to assure uniqueness
# the default value is for a private unregistered site, and may not provide
#   access to publicly exporting sites
# see http://vso.stanford.edu/netdrms/site_codes.html
DRMS_SITE_CODE  0x4000

# the default values for the remaining entries may or may  not be appropriate
#   for your site configuration, depending on how and where third-party
#   software has been set up and installed

# the user name of the postgres administrative account; normally "postgres"
#   if you have followed the PostgreSQL installation suggestions
POSTGRES_ADMIN  postgres

# the include path for the PostgreSQL API; likely to be either
#   /usr/include/pgsql or /usr/local/pgsql/include
#   it should contain the subdirectories: informix, internal, and server
POSTGRES_INCS   /usr/include/pgsql

# the location of the PostgreSQL libs; likely to be either
#   /usr/lib or /usr/lib64 or /usr/local/pgsql/lib
POSTGRES_LIBS   /usr/include/pgsql

# the user name of the SUMS administrator account - a special account is
#   recommended for multi-user systems, but not required
SUMS_MANAGER    production

# the host name of the default SUMS server you will be using; this is the
#   the machine that the SUMS storage units are mounted on, not necessarily
#   the machine serving the Postgres SUMS database
SUMS_SERVER_HOST        localhost

# the base directory for SUMS logs; used by base/sums/apps/sum_svc.c
SUMS_LOG_BASEDIR        /usr/local/logs/SUM

# the location of third-party libraries (especially cfitsio, which is required,
#   also others that may be used for modules such as fftw, gsl, etc.)
# if different libraries are in different paths, it is recommended that links
#   to all required ones be made in the single directory named here
THIRD_PARTY_LIBS        /usr/local/lib

# the location of third-party library include files (currently ignored);
#   see above for multiple locations
THIRD_PARTY_INCS        /usr/local/include

# a sample namespace appropriate to your site; this is only used for a
#   couple of database initialization scripts and is not important for
#   subsequent installations/updates
# see http://vso.stanford.edu/netdrms/site_codes.html
DRMS_SAMPLE_NAMESPACE   drms

# # # # # # 
# The following items are for Joe Hourcle's setup script, not used by 
# DRMS / SUMS directly

# The path of where to install the Postgres databases
POSTGRES_DATA_DIR	/usr/local/db/

# A space-separated list of paths where SUMS can store files
SUMS_PARTITIONS you_really_need_to_set_this

# The percentage of currently available space should SUMS try to use
#  on each partition it knows about
SUMS_FILL_TO_LINE	95

# The unix group that should own the SUMS directories
#  The files will be set writable to this whole group
#  if you run the generated scripts
SUMS_GROUP	sums

# The unix user that should own the SUMS directories
#  on a single-user system, this is the user's account
SUMS_USER	sums

# The directory where the NetDRMS is installed
DRMS	/home/jsoc/


