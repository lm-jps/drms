#!/home/jsoc/bin/linux_x86_64/perl5.12.2

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
    
    $lckfn = fileno($$lckfh);
    $lckfpath = $fpaths{$lckfn};
    
    flock($$lckfh, LOCK_UN);
    $$lckfh->close;
    
    if (defined($lckfpath))
    {
        chmod(0664, $lckfpath);
        delete($fpaths{$lckfn});
    }
}
1;