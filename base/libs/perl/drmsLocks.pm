#!/home/jsoc/bin/linux_x86_64/activeperl

package drmsLocks;

use Fcntl ':flock';
use FileHandle;

# Class global variables
my(%fpaths);

sub new
{
    my($clname) = shift;
    my($lockfile) = shift;
    my($err);
    my($fh);
    
    my($self) = 
    {
        _lockfh => undef
    };
    
    $err = 0;
    bless($self, $clname);
    
    if (defined($lockfile))
    {
        if ($self->AcquireLock($lockfile, \$fh))
        {
            $self->{_lockfh} = $fh;
        }
        else
        {
            print STDERR "Unable to acquire lock.\n";
            $err = 1;
        }
    }
    else
    {
        $err = 1;
    }
    
    if ($err)
    {
        $self = undef;
    }
    
    return $self;
}

sub DESTROY
{
    my($self) = shift;
    
    $self->ReleaseLock();
}

sub AcquireLock
{
    my($self) = shift;
    my($path) = shift;
    my($lckfh) = shift;
    my($gotlock);
    my($natt);
    
    if (-e $path)
    {
        $$lckfh = FileHandle->new("<$path");
        $fpaths{fileno($$lckfh)} = $path;
    }
    else
    {
        $$lckfh = FileHandle->new(">$path");
    }
    $gotlock = 0;
    
    $natt = 0;
    while (1)
    {
        if (flock($$lckfh, LOCK_EX|LOCK_NB)) 
        {
            $gotlock = 1;
            last;
        }
        else
        {
            if ($natt < 10)
            {
                print "Lock '$path' in use - trying again in 1 second.\n";
                sleep 1;
            }
            else
            {
                print "Couldn't acquire lock after $natt attempts; bailing.\n";
            }
        }
        
        $natt++;
    }
    
    return $gotlock;
}

sub ReleaseLock
{
    my($self) = shift;
    my($lckfh) = $self->{_lockfh};
    my($lckfn);
    my($lckfpath);
    
    if (defined($lckfh))
    {
        $lckfn = fileno($lckfh);
        $lckfpath = $fpaths{$lckfn};
        flock($$lckfh, LOCK_UN);
        
        $lckfh->close;
        $self->{_lockfh} = undef;
        
        if (defined($lckfpath))
        {
            chmod(0664, $lckfpath);
            delete($fpaths{$lckfn});
        }
    }
}

package drmsNetLocks;

use FileHandle;
use File::lockf;
use Data::Dumper;

# Class global variables
my(%fpaths);

sub new
{
    my($clname) = shift;
    my($lockfile) = shift;
    my($err);
    my($fh);
    
    my($self) = 
    {
        _lockfh => undef
    };
    
    $err = 0;
    bless($self, $clname);
    
    if (defined($lockfile))
    {
        if ($self->AcquireLock($lockfile, \$fh))
        {
            $self->{_lockfh} = $fh;
        }
        else
        {
            print STDERR "Unable to acquire lock.\n";
            $err = 1;
        }
    }
    else
    {
        $err = 1;
    }
    
    if ($err)
    {
        $self = undef;
    }
    
    return $self;
}

sub DESTROY
{
    my($self) = shift;
    
    $self->ReleaseLock();
}

sub AcquireLock
{    
    my($self) = shift;
    my($path) = shift;
    my($lckfh) = $_[0];
    my($gotlock);
    my($natt);
    
    $gotlock = 0;
    
    if (!(-e $path))
    {
        $$lckfh = FileHandle->new(">$path");
        $fpaths{fileno($$lckfh)} = $path;
    }
    else
    {
        $$lckfh = FileHandle->new(">$path");
    }
    
    if (defined($$lckfh))
    {
        $natt = 0;
        
        while (1)
        {
            if (!File::lockf::tlock($$lckfh, 0))
            {
                $gotlock = 1;
                last;
            }
            else
            {
                if ($natt < 10)
                {
                    print STDERR "Lock '$path' in use - trying again in 1 second.\n";
                    sleep 1;
                }
                else
                {
                    print STDERR "Couldn't acquire lock after $natt times; bailing.\n";
                    last;
                }
            }
            
            $natt++;
        }
    }
    else
    {
        print STDERR "Unable to open lock file for writing:$path\n";
    }
    
    return $gotlock;
}

sub ReleaseLock
{
    my($self) = shift;
    my($lckfh) = $self->{_lockfh};
    my($lckfn);
    my($lckfpath);

    if (defined($lckfh))
    {
        $lckfn = fileno($lckfh);
        $lckfpath = $fpaths{$lckfn};
        File::lockf::ulock($lckfh, 0);
        
        $lckfh->close;
        $self->{_lockfh} = undef;
        
        if (defined($lckfpath))
        {
            chmod(0666, $lckfpath);
            delete($fpaths{$lckfn});
        }
    }
}
1;
