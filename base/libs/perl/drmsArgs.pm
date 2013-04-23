#!/home/jsoc/bin/linux_x86_64/perl5.12.2

package drmsArgs;

use Getopt::Long;
use FileHandle;

# argsinH -  Input: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose values are the argument type (as specified in GetOptions()).
# argsoutH - Output: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose values are the argument values as specified on the cmd-line.
sub new
{
    my($clname) = shift;
    my($argsinH) = shift;
    my($req) = shift; # if 1, then these are required args, otherwise optional options.
                      # Defaults to args (req is optional).
    my($err);
    my($rv);
    
    my($self) = 
    {
        _argsinH => undef,
        _argsoutH => {},
        _req => undef
    };
    
    $err = 0;
    bless($self, $clname);
    
    if (defined($argsinH))
    {
        $self->{_argsinH} = $argsinH;
        
        if (!defined($req) || $req != 0)
        {
            $self->{_req} = 1;
        }
        else
        {
            $self->{_req} = 0;
        }
        
        if ($self->{_req} == 1)
        {
            # required args
            $rv = $self->ParseArgs();
        }
        else
        {
            # optional options
            $rv = $self->ParseOpts();
        }
        
        if (!defined($rv) || $rv == 0)
        {
            print STDERR "Invalid or missing cmd-line argument.\n";
            $err = 1;
        }
    }
    else
    {
        print STDERR "Invalid arguments.\n";
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
}

# Given a list of expected arguments, ParseArgs() parses the cmd-line and captures the expected-argument values.
# If an expected argument is missing, then ParseArgs() returns 0.
#
# Returns undef on error, 1 on success, and 0 if any argument name does not exist on the cmd-line.
# 
# NOTE: Each cmd-line argument name must match exactly one of the argument names in argsinH. No argument name
# can begin with '-' (GetOptions() considers that an invalid argument name).
sub ParseArgs
{
    my($self) = shift;
    my($argsinH) = $self->{_argsinH};
    my($argsoutH) = $self->{_argsoutH}; 
    
    my($rv) = 0;
    my(@argsA);
    my(@optlist);
    my($iarg);
    my($type);
    my($rsp);
    
    # Create hash array to pass to GetOptions
    @argsA = keys(%$argsinH);
    foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg};
        
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {    
            push(@optlist, "$iarg=$type");
        }
    }
    
    # Trick to get GetOptions() to ignore what we'd consider options. If an argument
    # name, from the cmd-line, starts with an optional space, then it matches, and 
    # GetOptions() will consider it an option and process it. It will also accept
    # things that start with '-', but in the @optlist, no argument will start with '-'.
    # If we want this to work for options too, defined as arguments that start with
    # '-', then each option in @optlist should start with '-'.
    # ( |) works for some reason too.
    # pass_through allows the skipping of unknown options (instead of causing GetOptions()
    # to fail.
    Getopt::Long::Configure("prefix_pattern=( ?)", "pass_through");
    
    # Returns undef if any options on cmd-line are not in @optlist. Returns 1 otherwise, even if
    # there is an item in @optlist that is not on the cmd-line.
    $rsp = GetOptions($argsoutH, @optlist);
    
    $rv = $rsp if (defined($rsp));
    
    if ($rv)
    {
        # Now check that all arguments in argsinH were actually specified on the cmd-line.
        my(@check);
        
        @argsA = keys(%$argsinH);
        @check = map ({(!defined($argsoutH->{$_}) ? $_ : ())} @argsA);
        $rv = ($#check < 0);
        
        unless ($rv)
        {
            my($msg) = join(', ', @check);
            print STDERR "Missing required argument(s) '$msg'.\n";
        }
    }
    
    return $rv;
}

sub ParseOpts
{
    my($self) = shift;
    my($argsinH) = $self->{_argsinH};
    my($argsoutH) = $self->{_argsoutH}; 
    
    my($rv) = 0;
    my(@argsA);
    my(@optlist);
    my($iarg);
    my($type);
    my($rsp);
    
    # Create hash array to pass to GetOptions
    @argsA = keys(%$argsinH);
    foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg}; 
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {
            push(@optlist, "$iarg=$type");            
        }
    }
    
    # A previous Configure call could mess everything up. Just always precede GetOptions()
    # with the proper configuration.
    Getopt::Long::Configure("default");
    Getopt::Long::Configure("pass_through");
    
    $rsp = GetOptions($argsoutH, @optlist);
    
    $rv = $rsp if (defined($rsp));
    
    return $rv;
}

# Returns argument value (as string)
sub Get
{
    my($self) = shift;
    my($name) = shift;
    my($rv);
    
    if (exists($self->{_argsoutH}->{$name}))
    {
        return $self->{_argsoutH}->{$name};
    }
    else
    {
        return undef;
    }
}
1;