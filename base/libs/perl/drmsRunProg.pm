#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use FileHandle;
use IPC::Open2;



package drmsSysRun;

sub RunCmd
{
    my($cmd) = shift;
    my($ret) = 0;
    my($sig);
    my($rsp);
    
    system($cmd);
    
    if ($? == -1)
    {
        print STDERR "Failed to execute '$cmd'.\n";
        $ret = -1;
    }
    else
    {
        $sig = $? & 127;
        $rsp = $? >> 8;
        
        if ($sig != 0)
        {
            print STDERR "$cmd terminated due to unhandled signal $sig.\n";
            $ret = -2;
        }
        elsif ($rsp != 0)
        {
            print STDERR "$cmd command returned error code $rsp.\n";
            $ret = 1;
        }
    }
    
    return $ret;
}

package drmsPipeRun;

sub new
{
    my($clname) = shift;
    my($cmd) = shift;
    
    my($self) = 
    {
        _fh => undef,
        _rfh => undef,
        _cmd => undef
    };

    bless($self, $clname);
    
    if (defined($cmd))
    {
        if ($self->OpenPipe($cmd))
        {
            $self = undef;
        }
    }
    

    return $self;
}

sub DESTROY
{
    my($self) = shift;
    $self->ClosePipe();
}

sub OpenPipe
{
    my($self) = shift;
    my($cmd) = shift;
    my($ret) = 0;
    my($fh);
    my($rfh);
    
    if (!defined($self->{_fh}))
    {
        IPC::Open2::open2($rfh, $fh, "$cmd 2>&1");

        if (0)
        {
            print STDERR "Unable to open pipe for reading/writing.\n";
            $ret = 1;
        }
        else
        {
            $self->{_fh} = $fh;
            $self->{_rfh} = $rfh;
        }
    }
    else
    {
        print STDERR "Pipe is already open for reading/writing.\n";
    }
    
    return $ret;
}

sub ReadPipe
{
    my($self) = shift;
    my($bufout) = shift;
    my($ret) = 0;
    my($fh);
    my($buf) = "";
    
    if (defined($self->{_rfh}))
    {
        $fh = $self->{_rfh};
        while (<$fh>)
        {
            $buf = $buf . $_;
        }
        
        $$bufout = $buf;
    }
    else
    {
        print STDERR "Can't read from unopened pipe.\n";
        $ret = 1;
    }
    
    return $ret;
}

sub WritePipe
{
    my($self) = shift;
    my($buf) = shift;
    my($ret) = 0;
    my($fh); # stupid perl.
    
    if (defined($self->{_fh}))
    {
        $fh = $self->{_fh};
        
        my($rsp) = print $fh $buf;
        
        unless ($rsp)
        {
            $ret = 1;
        }
    }
    else
    {
        print STDERR "Can't write to unopened pipe.\n";
        $ret = 1;
    }
    
    return $ret;
}

sub EnableAutoflush
{
    my($self) = shift;
    
    if (defined($self->{_fh}))
    {
        $self->{_fh}->autoflush(1);
    }
    else
    {
        print STDERR "Can't set autoflush on closed pipe.\n";
    }
}

sub ClosePipe
{
    my($self) = shift;
    my($which) = shift;
    my($rv) = 0;
    
    if (!defined($which) || $which == 1)
    {
        if (defined($self->{_fh}))
        {
            unless (close($self->{_fh}))
            {
                $rv = 1;
            }
            
            $self->{_fh} = undef;
        }
    }
    
    if (!defined($which) || $which == 0)
    {
        if (defined($self->{_rfh}))
        {
            unless (close($self->{_rfh}))
            {
                $rv = 1;
            }
            
            $self->{_rfh} = undef;
        }
    }
    
    return $rv;
}
1;