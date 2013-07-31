#!/home/jsoc/bin/linux_x86_64/activeperl

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
    my($type) = shift;
    
    my($self) = 
    {
        _fh => undef,
        _rfh => undef,
        _cmd => undef,
        _rstat => undef,
        _wstat => undef
    };

    bless($self, $clname);
    
    if (defined($cmd))
    {
        if ($self->OpenPipe($cmd, $type))
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
    my($type) = shift;
    my($ret) = 0;
    my($fh);
    my($rfh);
    
    if (!defined($self->{_fh}))
    {
        if (!defined($type))
        {
            IPC::Open2::open2($rfh, $fh, "$cmd 2>&1");
            
            if (0)
            {
                print STDERR "Unable to open pipe for reading/writing.\n";
                $ret = 1;
            }
        }
        elsif ($type == 0)
        {
            # read-only pipe
            unless (open($rfh, "$cmd 2>&1 |"))
            {
                print STDERR "Unable to open pipe for reading.\n";
                $ret = 1;
            }
        }
        elsif ($type == 1)
        {
            # write-only pipe
            unless (open($fh, "| $cmd 2>&1"))
            {
                print STDERR "Unable to open pipe for writing.\n";
                $ret = 1;
            }
        }

        $self->{_fh} = $fh;
        $self->{_rfh} = $rfh;
        $self->{_cmd} = $cmd;
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
    my($clret) = 0;
    my($status);
    
    if (!defined($which) || $which == 1)
    {
        if (defined($self->{_fh}))
        {
            # close will set $? to the return status of the process running on the pipe.
            # close will return 1 if the child process does not return 0.             
            $clret = close($self->{_fh});
            
            $status = $? >> 8;
            $self->{_wstat} = $status;
            $self->{_fh} = undef;
            
            if ($clret == 0 && $status == 0)
            {
                # close() failed (the child process returned 0, so close() must have had some problem).
                $rv = 1;
            }
        }
    }
    
    if (!defined($which) || $which == 0)
    {
        if (defined($self->{_rfh}))
        {
            # close will set $? to the return status of the process running on the pipe.
            # close will return 1 if the child process does not return 0.
            $clret = close($self->{_rfh});
            
            $status = $? >> 8;
            $self->{_rstat} = $status;
            $self->{_rfh} = undef;
            
            if ($clret == 0 && $status == 0)
            {
                # close() failed (the child process returned 0, so close() must have had some problem).
                $rv = 1;
            }
        }
    }
    
    return $rv;
}

sub GetStatus
{
    my($self) = shift;
    my($which) = shift;
    my($rv) = -1;
    
    if (!defined($which) || $which == 0)
    {
        return $self->{_rstat};
    }
    else
    {
        return $self->{_wstat};
    }
}
1;
