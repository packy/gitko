package FileUtils;
###########################################################################
#
# file utils
#
###########################################################################
use strict;
use warnings;


use File::Spec;
use File::Copy;
use File::Temp;
use File::Path;

use IO::File;
use IO::Dir;
use IO::Handle;

use POSIX qw(:sys_wait_h);
use Fcntl ':flock';

use Data::Dumper;
use Hash::Util;
use Carp;


#
# default is TRACE
#
$ENV{DEBUG_LEVEL} ||= 'TRACE';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init(
    { level => $ENV{DEBUG_LEVEL},
      file  => "STDOUT",
      layout=> "[\%d] [\%p]:\t\%m\%n",
    },
);

if ($^O eq 'MSWin32') {

    require 'Win32.pm';
    require 'Win32/Job.pm';
    require 'Win32/TieRegistry.pm';
    require 'Win32/FileSecurity.pm';
    require 'Win32/NetAdmin.pm';
    require 'Win32/API.pm';
}
###########################################################################
#
# globals
#
###########################################################################
my $HOSTNAME_BIN;
my $TEMPDIR;
my $SCP_BIN;

our $TAR_BIN = 'tar';
our $SSH_BIN = 'ssh';
our $SSH_OPTS =
  "-o FallBackToRsh=no -o PasswordAuthentication=no -o StrictHostKeyChecking=no -o NumberOfPasswordPrompts=0";
##########################################################################
#
# chown wrapper
#
##########################################################################
sub file_chown
{

    my ($username, @paths) = @_;

    if (scalar(@paths) == 0) {

        fatal("No path to chown.");
    }

    my ($uid, $gid) = (getpwnam($username))[2 .. 3];

    if (!defined $uid || !defined $gid) {

        fatal("Invalid user [$username]");
    }

    if (!chown($uid, $gid, @paths))
    {

        fatal("[FAILED]: chown [@paths]: $!");
    }
}

sub file_chmod
{
    my ($mode, @files) = @_;

    if (!chmod($mode, @files))
    {

        fatal("[ERROR] chmod [@files] failed: [$!]");
    }
}
###########################################################################
#
# generate a temp filename
#
###########################################################################
sub file_temp_filename
{

    my ($template, %args) = @_;

    if (!exists $args{DIR} || !$args{DIR})
    {
        $args{DIR} = dir_get_temp();

    } else {

        if (!-d $args{DIR}) {
            #
            # Create dir if needed
            #
            File::Path::mkpath($args{DIR}, 0, 755);
        }
    }

    my ($fh, $filename) = File::Temp::tempfile($template, %args);

    if (wantarray())
    {

        return ($fh, $filename);
    }
    #
    # close handle
    #
    $fh->close();
    undef $fh;

    return $filename;
}
##########################################################################
#
# find a suitable temp dir
#
##########################################################################
sub dir_get_temp
{

    if (exists $ENV{GA_LOG_DIR} && -d $ENV{GA_LOG_DIR})
    {
        return $ENV{GA_LOG_DIR};
    }

    if (defined $TEMPDIR && -d $TEMPDIR)
    {
        return $TEMPDIR;
    }

    $TEMPDIR = File::Temp::tempdir(
        'extra_clarity_XXXXXX',
        DIR     => File::Spec->tmpdir(),
        CLEANUP => 0
    );

    chmod(0777, $TEMPDIR);

    FileUtils::log("Will use tempdir [$TEMPDIR]");

    return $TEMPDIR;
}
##########################################################################
#
# Find an executable in GA_INSTALL_USER's PATH
#
##########################################################################
sub file_find_in_path
{

    my $cmd = shift;

    foreach my $path (File::Spec->path())
    {

        if (-x "$path/$cmd")
        {
            FileUtils::log("[$cmd] found in [$path]");
            return "$path/$cmd";
        }
    }
    #
    # "Make it work" even if $cmd is not in oinstall's PATH
    #
    if ($^O eq 'solaris' and -x "/usr/ccs/bin/$cmd")
    {

        FileUtils::log(
            "[$cmd] not in PATH, will use the one in [/usr/ccs/bin]");
        return "/usr/ccs/bin/$cmd";
    }

    fatal("cannot find [$cmd] executable. Adjust your PATH and try again.");
}

sub file_find_first_line
{

    my ($file, $regex) = @_;

    my $callback = sub {

        my ($line, $ret) = @_;

        if ($line =~ $regex)
        {

            $$ret = $line;
            return 0;
        }

        return 1;
    };

    my $ret;
    file_foreach_line($file, $callback, \$ret);

    return $ret;
}
##########################################################################
#
# execute non-interactive script/command
#
# Args: %context
#       cmd     - string   - command to execute (cp)
#       args    - arrayref - command arguments  ([-vf filename])
#       user    - string   - user to run as
#       env     - hashref  - global environment variables for the command
#       dir     - string   - directory to chdir to when running the command
#       source  - string   - path to file to `source' before executing command
#       continue_on_error
#               - boolean  - do not die if exit code is non-0. Default is false.
#       expect  - perl regex - die if no line in the command's output matches
#                            used to validate successful execution of the command
#
# Return value: hashref
#       stdout  - string    - path to file with the command output
#       stderr  - string    - path to file with the command stderr
#       exitcode- number    - command exit code. 0 usually means success.
#
##########################################################################
sub file_execute
{

    my (%context) = @_;

    return (exists $context{rhost} && $context{rhost})
      ? file_execute_remote(%context)
      : __file_execute(%context);
}

sub __file_execute
{

    my (%context) = @_;

    $context{stdout} = file_temp_filename(
        'cmd_XXXXXX',
        SUFFIX => '.stdout',
        DIR    => exists $context{logdir} && $context{logdir}
            ? $context{logdir} : undef,
        UNLINK => 0
    );

    $context{stderr} = file_temp_filename(
        'cmd_XXXXXX',
        SUFFIX => '.stderr',
        DIR    => exists $context{logdir} && $context{logdir}
            ? $context{logdir} : undef,
        UNLINK => 0
    );

    $context{exitcode} = undef;

    _file_build_script_from_command(\%context);

    FileUtils::log("Command context before execution " . Dumper(\%context));

    my $pid = -1;

    if ($^O eq 'MSWin32') {
        #
        # execute script
        #
        _child_exec_win32(\%context);

    } else {
        #
        # fork to exec
        #
        $pid = fork();
        #
        # error
        #
        defined $pid or fatal("fork() failed");
        #
        # child
        #
        !$pid and _child_exec_unix(\%context);
        #
        # parent
        #
        waitpid($pid, 0);

        $context{exitcode} = $? >> 8;
    }
    #
    # no more changes to %context
    #
    Hash::Util::lock_keys(%context);
    #
    # dump child's output
    #
    __file_execute_report($pid, \%context)
        unless exists $context{nolog} && $context{nolog};
    #
    # by default die on exitcode != 0 unless explicitly requested
    #
    if (exists $context{expect} && defined $context{expect})
    {
        if (!defined file_find_first_line($context{stdout}, $context{expect}))
        {
            if (exists $context{on_error} && $context{on_error}) {
                #
                # call error handler
                #
                $context{on_error}->(\%context);
            }

            fatal(
                "Command [$context{cmd}] failed because we did not find expected results"
            );
        }
    }

    if (!exists $context{continue_on_error} || !$context{continue_on_error})
    {
        if ($context{exitcode})
        {
            if (exists $context{on_error} && $context{on_error}) {
                warn "Caught error. Context before calling on_error handler: " . Dumper(\%context);

                #
                # call error handler
                #
                $context{on_error}->(\%context);
            }

            fatal("command [$context{cmd}] failed.");
        }
    }

    return \%context;
}

sub __file_execute_report
{
    my ($pid, $context) = @_;

    trace("Process [$pid] exited with code [$context->{exitcode}]");
    trace("-------------------------------------------------------");

    my $max_lines = 20;
    my $cb = sub {

        my $line = shift;

        if ($max_lines-- == 0) {
            return 0;
        }

        trace($line);

        return 1;
    };

    trace("*** STDOUT ***");

    if (exists $context->{spool}) {

        file_foreach_line($context->{stdout}, $cb);
        trace("Output truncated");

    } else {

        file_foreach_line($context->{stdout}, sub {trace($_[0]);});
    }

    trace("*** END STDOUT ***");

    trace("*** STDERR ***");
    if (exists $context->{spool}) {

        file_foreach_line($context->{stderr}, $cb);
        trace("Output truncated");

    } else {

        file_foreach_line($context->{stderr}, sub {trace($_[0]);});

    }

    trace("*** END STDERR ***");
    trace("End log for pid [$pid]");
}

sub file_execute_remote
{
    my (%context) = @_;

    $SSH_BIN = file_find_in_path('ssh')
      unless defined $SSH_BIN;

    my $remote_env = '';

    if (exists $context{renv})
    {

        foreach my $var (keys %{$context{renv}})
        {

            $remote_env .= "$var=$context{renv}->{$var} && export $var && ";
        }
    }

    my $rdir = ' ';

    if (exists $context{rdir})
    {

        $rdir = "cd $context{rdir} && ";
    }

    my $rcmd = $remote_env . $rdir . $context{cmd};

    if (exists $context{args} && defined $context{args} && scalar(@{$context{args}}))
    {

        $rcmd .= ' ' . join(' ', @{$context{args}});
    }

    if (exists $context{ruser} && defined $context{ruser})
    {

        $rcmd = "su $context{ruser} -c '$rcmd'";
    }

    $context{cmd} = $SSH_BIN;
    $context{args} = [$SSH_OPTS, $context{rhost}, "\"$rcmd\""];

    return __file_execute(%context);
}
###########################################################################
#
# get hostname
#
###########################################################################
sub hostname_short
{

    my $hostname = shift;
    # greedy at the end
    $hostname =~ s/\..*$//;

    return $hostname;
}

sub host_get_hostname
{

    my $short = shift;

    my $hostname;

    if (!defined $HOSTNAME_BIN)
    {
        $HOSTNAME_BIN = file_find_in_path('hostname');
    }

    my $res = file_execute(cmd => $HOSTNAME_BIN);
    if ($res->{exitcode})
    {
        fatal("[$HOSTNAME_BIN] failed: [$!]");
    }

    file_foreach_line($res->{stdout}, sub {$hostname = shift; 0;});

    $hostname = hostname_short($hostname)
      if defined $short && $short;

    FileUtils::log("will use hostname [$hostname]");

    return $hostname;
}
###########################################################################
#
# private
#
###########################################################################
sub get_groups_for_user
{
    my $user = shift;
    my @gids = ();

    while (my @groupinfo = getgrent())
    {
        push @gids, $groupinfo[2] if $groupinfo[3] =~ /\b$user\b/;
    }
    endgrent();

    return @gids;
}

sub _child_exec
{
    return $^O eq 'MSWin32' ? _child_exec_win32(@_)
                            : _child_exec_unix(@_);
}

sub _child_exec_unix
{
    my ($context) = shift;
    #
    # reopen STDOUT & STDERR
    #
    close(STDOUT);
    close(STDERR);
    open(STDOUT, '>', $context->{stdout})
      or fatal("Cannot open [$context->{stdout}]: [$!]");
    STDOUT->autoflush(1);

    open(STDERR, '>', $context->{stderr})
      or fatal("Cannot open [$context->{stderr}]: [$!]");
    STDERR->autoflush(1);
    #
    # run command
    #
    if (exists $context->{script} && defined $context->{script})
    {
        my $shell = $context->{script};

	    if (exists $context->{user} && defined $context->{user} && $context->{user} ne 'root') {

            if (exists $context->{inherit_env} && $context->{inherit_env}) {

                $shell = "su - $context->{user} -c $shell";

            } else {

                $shell = "su $context->{user} -c $shell";
            }
	    }

        exec($shell)
        #
        # exec itself failed (not the command)
        #
        or fatal("[exec $context->{cmd}] failed: [$!]");
    }
    #
    # run coderef
    #
    elsif (exists $context->{funcref})
    {

        if (ref $context->{funcref} ne 'CODE')
        {

            fatal("Not a coderef!");
        }

    	if (exists $context->{env} && defined $context->{env})
    	{
        	#
        	# set environment
        	#
        	map {$ENV{$_} = $context->{env}->{$_}} (keys %{$context->{env}});
    	}

    	if (exists $context->{dir} && defined $context->{dir})
    	{
            chdir($context->{dir})
                or fatal("chdir [$context->{dir}] failed: [$!]");
    	}

        $context->{funcref}->(
            (exists $context->{args} && defined $context->{args})
            ? @{$context->{args}}
            : undef
        );
        exit 0;

    } else {

        fatal("Neither command nor coderef supplied, nothing to do.");
    }
}

sub _child_exec_win32 {

    my ($context) = shift;
    #
    # run command
    #
    if (exists $context->{script} && defined $context->{script}) {

        # based on http://ss64.com/nt/cmd.html
        my $script = 'cmd.exe /C ' . "\"$context->{script}\"";
        #
        # run as $context->{user}. Also requires password.
        #
        if (exists $context->{user} && defined $context->{user}) {
            #
            # cheater
            #
            my $su = $ENV{GA_PERL};
            $su =~ s/\\perl\.exe$/\\run_as_user\.exe/i;

            my $domain = '.';
            my $user   = $context->{user};
            my $passwd = $context->{passwd};

            if ($user =~ /\\/) {

                ($domain, $user) = split /\\/, $user, 2;
            }

            $script = "$su \"$user\" \"$domain\" \"$passwd\" \"$script\"";
        }

        my $job = Win32::Job->new();

        $job->spawn('cmd.exe', $script, {no_window => 1,
                                        stdout     => $context->{stdout},
                                        stderr     => $context->{stderr}});
        #
        # wait until all done.
        #
	$job->run(-1, 1);

        my $stat = $job->status();

        $context->{exitcode} = 0;

        foreach my $pid (keys(%$stat)) {

            $context->{exitcode} |= $stat->{$pid}->{exitcode};
        }

        return;
    }
    #
    # run coderef
    #
    if (exists $context->{funcref}) {

        if (ref $context->{funcref} ne 'CODE') {

            fatal("Not a coderef!");
        }

        $context->{funcref}->((exists $context->{args} && defined $context->{args})
                ? @{$context->{args}}
                : undef);
        exit 0;
    }
}

sub _file_build_script_from_command {

    return ($^O eq 'MSWin32')
        ? _file_build_script_from_command_win32(@_)
        : _file_build_script_from_command_unix(@_);
}

sub _file_build_script_from_command_unix
{

    my $context = shift;

    my $shell = file_temp_filename(
        'cmd_XXXXXX',
        UNLINK => 0,
        SUFFIX => '.sh'
    );

    open(my $SHELL, '>', $shell)
      or fatal("Cannot open [$shell]: [$!]");

    print $SHELL "#!/bin/sh\n\n";
    #
    # pre-set env
    #
    if (exists $context->{source} && defined $context->{source}) {

        print $SHELL "\n. $context->{source}\n";
    }
    #
    # set env
    #
    if (exists $context->{env} && defined $context->{env}) {

    	foreach my $VAR (keys %{$context->{env}}) {

            next if ($^O eq 'aix' && $VAR eq 'LOGNAME');

            my $val = $context->{env}->{$VAR};
            $val =~ s/\"/\\\"/g;

            print $SHELL "$VAR=\"$val\"\n";
            print $SHELL "export $VAR\n\n";
        }
    }

    if (exists $context->{home} && defined $context->{home})
    {
        print $SHELL "cd $context->{home}\n\n";
    }
    elsif (exists $context->{dir} && defined $context->{dir})
    {
        print $SHELL "cd $context->{dir}\n\n";
    }

    my $args = '';

    if (exists $context->{args} && defined $context->{args})
    {
        $args .= ' ' . join(' ', @{$context->{args}});
    }

    my $line = $context->{cmd} . $args;

    print $SHELL "exec $line\n";

    close $SHELL;

    file_chmod(0755, $shell);

    $context->{script} = $shell;

    return $shell;
}

sub _file_build_script_from_command_win32 {

    my $context = shift;

    my ($pl, $shell) = file_temp_filename(
        'cmd_XXXXXX',
        UNLINK => 0,
        SUFFIX => '.pl',
        exists($context->{DIR}) ? (DIR => $context->{DIR}) : ()
    );

    print $pl "use strict;\nuse warnings;\n";

    print $pl 'my ' . Data::Dumper->Dump([$context->{env}], ['$env']) . "\n";
    print $pl '@ENV{keys %$env} = values %$env;', "\n";

    if (exists $context->{home} && defined $context->{home}) {
        print $pl 'chdir("', quotemeta($context->{home}), '");', "\n";
    }

    my $cmd  = $context->{cmd};
    my $args = [];
    if (exists $context->{args} && defined $context->{args}) {
        $args = $context->{args};
        fix_arguments_list($args);
    }

    print $pl 'my $cmd_line = "', quotemeta($context->{cmd}) . '";', "\n";
    print $pl 'my ', Data::Dumper->Dump([$args], ['$args']) . "\n";

    print $pl q/my $args_line = join(' ', @$args);/, "\n";
    # system invoked without LIST approach, because it tries split LIST values with spaces,
    # doesn't matter we expected they shouldn't be modified
    print $pl 'system qq/$cmd_line $args_line/ and exit $? >> 8;', "\n";
    print $pl 'exit 0;',                                    "\n";

    # Based on http://ss64.com/nt/cmd.html in order to make invation of cmd more safe
    # we should quote each argument
    $context->{script} = "\"$ENV{GA_PERL}\" \"$shell\"";

    return $shell;
}

#
# traverse the path, resolving any symlink
# NOTE: only dirs supported
#
sub resolve_path
{

    my $path = shift;

    fatal("Invalid path [$path], path doesn't exist.")
      unless -e $path;

    my @dirs = File::Spec->splitdir($path);

    my $realpath = File::Spec->rootdir();

    foreach my $dir (@dirs)
    {

        my $current = File::Spec->catdir($realpath, $dir);

        my $recursion_depth = 10;

        while (-l $current)
        {

            if ($recursion_depth-- == 0)
            {
                fatal("Recursive symlink detected in [$path], exitting.");
            }

            my $resolved = readlink($current)
              or fatal("cannot readlink [$current]: [$!]");

            FileUtils::log("[$current] resolves to [$resolved]");
     #
     # The symlink could be an absolute or a local path, we have to handle both
     #
            if (File::Spec->file_name_is_absolute($resolved))
            {

                $current = $resolved;

            }
            else
            {

                my @path = File::Spec->splitdir($current);
                pop @path;

                $current = File::Spec->catdir(@path, $resolved);
            }
        }

        $realpath = $current;
    }

    fatal("broken symlink in [$realpath].")
      unless -e $realpath;

    return File::Spec->canonpath($realpath);
}

sub file_append {

    my ($filename, $line) = @_;

    open(my $FILEX, '>>', $filename)
        or fatal("cannot open file [$filename] for append: [$!]");

    flock($FILEX, LOCK_EX);
    print $FILEX "$line\n";
    flock($FILEX, LOCK_UN);

    close $FILEX;
}
##################################################################
#
# Hardcoded:
#  # is a comment
#
##################################################################
sub file_read_as_hash {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my %hash = ();
    my $file;
    #
    # This check will reduce the log
    #
    if (exists $args{rhost} && defined $args{rhost}) {

        my $ret = file_execute(
            cmd   => 'cat',
            args  => [$args{file}],
            rhost => $args{rhost},
            user  => $args{user},
        );

        $file = $ret->{stdout};

    } else {

        $file = $args{file};
    }

    file_foreach_line(
        $file,
        sub {

            my ($line, $hash) = @_;
            #
            # remove comments
            #
            $line =~ s/\s*\#.*$//;

            return 1 if !$line;

            #my ($key, $value) = split(/\s*\=\s*/, $line, 2);
            my ($key, $value) = split($args{separator}, $line);
            #
            # trim
            #
            $key   = trim($key);
            $value = trim($value);

            $hash->{$key} = $value;

            return 1;
        },
        \%hash
    );
    #
    # expand vars
    #
    foreach my $key (keys %hash)
    {
        $hash{$key} =~ s/\$\{$_\}/$hash{$_}/g foreach (keys %hash);
    }

    FileUtils::log("file [$args{file}] " . Dumper(\%hash));

    return \%hash;
}

sub trim {

    my $val = shift;

    $val =~ s/^\s+//;
    $val =~ s/\s+$//;

    return $val;
}

sub file_save_from_hash
{

    my ($file, $hash) = @_;

    my $fh = IO::File->new("> $file")
      or fatal("cannot open [$file]: [$!]");

    foreach my $key (keys %$hash)
    {

        $fh->print("$key=$hash->{$key}\n");
    }

    $fh->close();
}
##########################################################################
#
# will run $funcref in separate process not exceeding $MAX_PROCS
# Return: 0 on success, non-zero if one of the kids failed
#
##########################################################################
sub file_execute_parallel
{

    my ($MAX_PROCS, $funcref, @funcargs) = @_;
    my $ret   = 0;
    my $procs = 0;

    my %kids = ();

    foreach my $arg (@funcargs)
    {

      AGAIN:
        while ($procs >= $MAX_PROCS)
        {
            #
            # MAX_PROCS exhausted, wait for a child to finish
            #
            FileUtils::log(
                "PARENT - MAX_PROCS [$procs] reached, waiting for a process to exit..."
            );

            if ((my $pid = wait()) != -1)
            {

                my $err = $? >> 8;
                FileUtils::log("child [$pid] exited with code [$err]");

                $ret |= $err;
                $procs--;

            }
            else
            {
                #
                # no kids running
                #
                $procs = 0;
            }
        }
        #
        # kids output:
        #
        my %context = (
            exitcode => undef,
            user     => undef,
            funcref  => $funcref,
            args     => [$arg],
        );

        $context{'stdout'} = file_temp_filename(
            'execute_parallel_XXXXXX',
            SUFFIX => '.stdout',
            UNLINK => 0
        );
        $context{'stderr'} = file_temp_filename(
            'execute_parallel_XXXXXX',
            SUFFIX => '.stderr',
            UNLINK => 0
        );
        Hash::Util::lock_keys(%context);

        my $pid = fork();

        if (!defined $pid)
        {

            FileUtils::log("PARENT - fork failed: [$!], will trying again...");
            goto AGAIN;

        }
        elsif ($pid)
        {
            #
            # parent
            #
            $procs++;
            #
            # store kids stdout & stderr locations
            #
            $kids{$pid} = \%context;

        }
        else
        {
            #
            # child
            #
            _child_exec(\%context);
        }
    }
    #
    # no more sids, wait for the kids
    #
    while ((my $pid = wait()) != -1)
    {

        my $err = $? >> 8;
        FileUtils::log("child [$pid] exited with [$err]");

        $kids{$pid}->{exitcode} = $err;

        $ret |= $err;
    }
    #
    # report kids status
    #
    FileUtils::log("All processes finished.");
    #
    # merge kids stdout/stderr with the log
    #
    foreach my $pid (keys %kids)
    {
        __file_execute_report($pid, $kids{$pid});
    }

    return $ret;
}

sub trace {

    my $message = shift;

    TRACE($message);
}

sub log {

    my $message = shift;

    INFO($message);
}

sub fatal {

    my $message = shift;

    LOGCONFESS($message);
}

sub file_foreach_line {

    my ($filename, $callback, $args) = @_;

    my $fh = IO::File->new($filename)
        or fatal("cannot open [$filename]: [$!]");

    while (my $line = <$fh>) {

        chomp $line;
        #
        # interrupt if callback returns 0
        #
        last unless $callback->($line, $args);
    }

    $fh->close();
}

sub dir_foreach_entry {

    my ($dir, $callback, $args) = @_;

    my $dh = IO::Dir->new($dir)
        or fatal("cannot open dir [$dir]: [$!]");

    my @entries = ();
    while (my $entry = $dh->read()) {

        chomp $entry;
        push @entries, $entry;
    }

    $dh->close();

    foreach my $entry (sort @entries) {
        #
        # interrupt if callback returns 0
        #
        last unless $callback->($entry, $args);
    }
}

sub file_test_remote {

    return file_test(@_);
}

sub file_test {

    return $^O ne 'MSWin32'
        ? __file_test_unix(@_)
        : __file_test_win32(@_);
}

sub __file_test_unix {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $result = file_execute(
        cmd               => '/usr/bin/test',
        args              => [$args{TEST}, $args{FILE}],
        ruser             => exists $args{LOCAL_USER} && defined $args{LOCAL_USER} ? undef : exists $args{USER} ? $args{USER} : undef,
        user              => exists $args{LOCAL_USER} && defined $args{LOCAL_USER} ? $args{LOCAL_USER} : undef,
        rhost             => $args{NODE},
        continue_on_error => 1,
    );

    return $result->{exitcode} == 0;
}

sub __file_test_win32 {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $result = file_execute(
        cmd               => 'cmd.exe',
        args              => ["/C dir \"$args{FILE}\""],
        ruser             => exists $args{LOCAL_USER} && defined $args{LOCAL_USER} ? undef : exists $args{USER} ? $args{USER} : undef,
        user              => exists $args{LOCAL_USER} && defined $args{LOCAL_USER} ? $args{LOCAL_USER} : undef,
        passwd            => exists $args{PASSWD} && $args{PASSWD} ? $args{PASSWD} : '',
        rhost             => exists $args{NODE} ? $args{NODE} : undef,
        continue_on_error => 1,
    );

    return $result->{exitcode} == 0;
}

sub file_copy_remote {

    return $^O ne 'MSWin32'
        ? __file_copy_remote_unix(@_)
        : __file_copy_remote_win32(@_);
}

sub __file_copy_remote_unix {

    my $nodes = shift;

    $SCP_BIN = file_find_in_path('scp')
        unless defined $SCP_BIN;

    my $callback = sub {

        my $node = shift;

        if (exists $node->{FORCE} && !$node->{FORCE}) {

            if (file_test(
                TEST => '-e',
                USER => $node->{OWNER},
                LOCAL_USER => $node->{SSH_USER},
                FILE => $node->{DEST_FILE},
                NODE => $node->{NODE})) {

                FileUtils::log(
                    "Destination file [$node->{DEST_FILE}] already exists, will not copy without FORCE."
                );
                return;
            }
        }

        my $ret = __file_execute_unix(
            cmd  => $SCP_BIN,
            args => [
                $SSH_OPTS, $node->{SRC_FILE},
                "$node->{NODE}:$node->{DEST_FILE}"
            ],
            user => $node->{SSH_USER},
        );

        if ($ret->{exitcode}) {

            fatal("cannot copy [$node->{SRC_FILE}] to [$node->{NODE}:$node->{DEST_FILE}]");
        }

        if (exists $node->{OWNER} && defined $node->{OWNER} && $node->{OWNER} ne $node->{SSH_USER})
        {
            file_chown_remote(
                rhost   => $node->{NODE},
                owner   => $node->{OWNER},
                group   => exists $node->{GROUP} ? $node->{GROUP} : undef,
                files   => [$node->{DEST_FILE}],
                user    => $node->{SSH_USER},
            );
        }
    };

    if (file_execute_parallel(6, $callback, @$nodes))
    {
        fatal("failed to copy file to remote node");
    }
}

sub __file_copy_remote_win32 {

    my $nodes = shift;

    foreach my $node (@$nodes) {

        if (exists $node->{FORCE} && !$node->{FORCE}) {
            #
            # TODO: don't overwrite without FORCE
            #
        }
        #
        # src
        #
        my ($src_vol, $src_dir, $src_file) = File::Spec->splitpath($node->{SRC_FILE});
        my $src_path = File::Spec->catdir($src_vol, $src_dir);
        #
        # dest
        #
        my ($dest_vol, $dest, undef) = File::Spec->splitpath($node->{DEST_FILE});
        $dest = File::Spec->catdir($dest_vol, $dest);

        FileUtils::log("Will copy from [$node->{DEST_FILE}] to [$dest]");

        file_execute(
            cmd  => File::Spec->catfile($ENV{GA_SCRIPT_DIR}, 'tools', 'robocopy.exe'),
            #
            # /NP   - No Progress bar
            # /R:n  - num retries   (default 1,000,000)
            # /W:n  - wait between retries (default 30 secs)
            #
            args => ["/NP /R:15 /W:5 \"$src_path\" \"$dest\" $src_file"],
            user => $node->{SSH_USER},
            passwd => $node->{SSH_PASSWD},
            # returns 1 on success
            continue_on_error => 1,
            # we always copy 1 file
            #     Files :         1         1         0         0         0         0
            expect => qr/^\s+Files \:\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+$/,
        );
    }
}

sub user_primary_group
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $ret = file_execute(
        cmd   => 'id',
        args  => ['-gn', $args{ruser}],
        rhost => $args{rhost},
        user  => $args{user},
    );

    if ($ret->{exitcode})
    {

        fatal("remote command failed.");
    }

    my $rgroup;

    file_foreach_line($ret->{stdout}, sub {$rgroup = shift; return 0;},);

    if (!defined $rgroup)
    {

        fatal("cannot find remote user [$args{ruser}] primary group");
    }

    return $rgroup;
}

sub file_chown_remote
{
    my (%args) = @_;

    my $group = '';

    if (!exists $args{group}) {

        $group = user_primary_group(
            rhost => $args{rhost},
            ruser => $args{owner},
            user  => $args{user},
        );

    } else {

        $group = $args{group};
    }

    my $ret = file_execute(
        cmd   => 'chown',
        args  => ["$args{owner}:$group", @{$args{files}}],
        rhost => $args{rhost},
    );

    if ($ret->{exitcode})
    {

        fatal("Unable to chown on host [$args{rhost}]");
    }
}

sub retry_until
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $max_tries = $args{MAX_TRIES};

    while ($max_tries--)
    {
        #
        # interrupt on return 1 from callback
        #
        return 1
          if ($args{CALLBACK}->($args{CALLBACK_ARG}));

        FileUtils::log("Try [$max_tries] in [$args{INTERVAL}] secs...");

        sleep($args{INTERVAL});
    }

    FileUtils::log("Failed after [$args{MAX_TRIES}] attempts.");

    return 0;
}

sub dir_find_owner
{
    my $dir = shift;

    if (!-d $dir)
    {

        FileUtils::log("Path [$dir] doesn't exist.");
        return undef;
    }

    my $uid = (stat($dir))[4];

    if (!defined $uid)
    {

        FileUtils::log("Path [$dir]: invalid owner.");
        return undef;
    }

    my $owner = getpwuid($uid);

    if (!defined $owner)
    {

        FileUtils::log("Invalid userid [$uid]");
        return undef;
    }

    return $owner;
}

sub win32_registry_read_key {

    my ($path, $key) = @_;

#    FileUtils::log("Will fetch key [$path/$key]...");

    my @flags = ();
    #
    # KEY_WOW64_64KEY and KEY_WOW64_32KEY not defined on Win2000
    #
    eval {

        push @flags, Win32::TieRegistry::KEY_READ() |
                     Win32::TieRegistry::KEY_WOW64_64KEY();
	};

    eval {

        push @flags, Win32::TieRegistry::KEY_READ() |
                     Win32::TieRegistry::KEY_WOW64_32KEY();
    };

    push @flags, Win32::TieRegistry::KEY_READ();

    foreach my $access (@flags) {

        my $hive  = Win32::TieRegistry->Open($path, { Access => $access, Delimiter => "/" });
        next unless $hive;

        $hive = $hive->TiedRef();
        my $result = $hive->{$key};

        if (defined $result) {

            return ref($result) ? %$result : $result;
        }
    }

    return;
}

sub win32_registry_get_key_values {

    my ($path, $key) = @_;

    my @flags;
    my %results;

    eval {
        push @flags,    Win32::TieRegistry::KEY_READ() |
                        Win32::TieRegistry::KEY_WOW64_64KEY();
    };

    eval {
        push @flags,    Win32::TieRegistry::KEY_READ(),
                        Win32::TieRegistry::KEY_WOW64_32KEY();
    };

    foreach my $access ( @flags ) {

        my $hive = Win32::TieRegistry->Open("$path\/$key", { Access => $access, Delimiter => '/'});
        next unless $hive;

        my @names = $hive->ValueNames();
        @results{@names} = map { ($hive->GetValue($_))[0] } @names;

        return \%results;

    }

    return {};

}

sub win32_registry_set_key {

    my ($path, $key, $value) = @_;

    my @flags = ();
    #
    # KEY_WOW64_64KEY and KEY_WOW64_32KEY not defined on Win2000
    #
    eval {

        push @flags, Win32::TieRegistry::KEY_READ()  |
                     Win32::TieRegistry::KEY_WRITE() |
                     Win32::TieRegistry::KEY_WOW64_64KEY();
    };

    eval {

        push @flags, Win32::TieRegistry::KEY_READ() |
                     Win32::TieRegistry::WRITE() |
                     Win32::TieRegistry::KEY_WOW64_32KEY();
    };

    push @flags, Win32::TieRegistry::KEY_READ() |
                 Win32::TieRegistry::KEY_WRITE();

    foreach my $access (@flags) {

        my $hive = Win32::TieRegistry->Open($path, { Access => $access,
                                                     Delimiter => "/",
                                                   });
        next unless $hive;

        if (!$hive->SetValue($key, $value)) {

            fatal("Failed to set registry key [$path/$key]");
        }

        return;
    }

    fatal("Failed to set registry key [$path/$key]");
}

# On UNIX returns hash with all group members => rights
# On Win32 returns hash of username => rights
sub users_granted_access_on_directory {
    if ($^O eq 'MSWin32') {
        return _win32_users_granted_access_on_directory(@_);
    }
    else {
        return _unix_users_granted_access_on_directory(@_);
    }
}

my @perms        = qw(--- --x -w- -wx r-- r-x rw- rwx);

sub _unix_users_granted_access_on_directory {
    my $dir = shift;

    my %users;

    my @stat = stat $dir;

    my $mode      = $stat[2];
    my $groupmode = ($mode & 00070) >> 3;

    if ($groupmode) {
        my $groupmodetxt = $perms[$groupmode];

        my $group = getgrgid $stat[5];
        %users = map { $_ => $groupmodetxt } _unix_get_users_of_group($group);
    }

    my $owner = getpwuid $stat[4];

    $users{$owner} = 'owner';

    return %users;
}

sub _unix_get_users_of_group {
    my $group = shift;

    my @existing_users;
    while (my $user = getpwent()) {
        push @existing_users, $user;
    }

    my @users;
    foreach my $name (@existing_users) {
        my $usergrouptext = `id -Gn $name`;
        chomp $usergrouptext;

        my $belongs = grep /^\Q$group\E$/, split(/\s+/, $usergrouptext);

        # User does not belong to $group
        next unless $belongs;

        push @users, $name;
    }

    endpwent();

    return @users;
}

sub _win32_users_granted_access_on_directory {
    my $dir = shift;
    my %acl;
    Win32::FileSecurity::Get($dir, \%acl);

    foreach my $user (keys %acl) {
        my @rights;
        Win32::FileSecurity::EnumerateRights($acl{$user}, \@rights);
        $acl{$user} = join ', ', @rights;
    }
    return %acl;
}

sub is_dir_accessible_by_world {
    my $dir = shift;

    my %result;

    if ($^O !~ /MSWin/i) {

        my @stat = stat $dir;
        my $mode = $stat[2];

        # If permissions are granted for world access, this is a Finding.
        if ($mode & 00007) {
            $result{others} = $perms[$mode & 00007];
        }
        else {
            return;
        }
    }
    else {
        my %users_with_access =
          FileUtils::users_granted_access_on_directory($dir);
        next unless exists $users_with_access{Everyone};

        $result{Everyone} = $users_with_access{Everyone};
    }

    return wantarray ? %result : 1;
}

sub is_dir_accessible_by_users_not_from_list {
    my $dir = shift;
    my %allowed_users = map { $_ => 1 } @_;

    my %users_with_access =
      FileUtils::users_granted_access_on_directory($dir);

    my @disallowed = grep !exists($allowed_users{$_}),
      keys %users_with_access;

    return unless @disallowed;

    my %result = map { $_ => $users_with_access{$_} } @disallowed;

    return wantarray ? %result : 1;
}

sub file_slurp {

    my $file = shift;
    my $content;
    local $/;
    open(FL, '<', $file) or fatal("Can not open a file [$file]!");
    $content = <FL>;
    close FL;

    return $content;

}

sub file_rewrite {
    my $file = shift;
    my $content = shift;
    fatal("File and new content must be defined\n") unless ($content && $file);

    open(FL, '>', $file) or fatal("Can not open a file [$file]!");
    flock(FL, LOCK_EX);
    print FL $content;
    flock(FL, LOCK_UN);
    close FL;

}

sub get_group_members {

    my $group = shift;
    my $members = {};

    if ($^O eq 'MSWin32') {
        Win32::NetAdmin::LocalGroupGetMembersWithDomain('', $group, $members);
        %{$members} = map { $_ => 1} grep { $members->{$_} == 1 } keys %{$members};
    } else {
            %{$members} = map { $_ => 1 } _unix_get_users_of_group($group);
    }
    return $members;

}

sub is_group_member {

    my $group = shift;
    my $user = shift;

    my $group_members = get_group_members($group);
    return scalar grep { $_ eq $user } keys %{$group_members};

}

sub get_local_groups {

    my @localgroups;

    my $is_powershell =
                win32_registry_read_key('LMachine/SOFTWARE/microsoft/powershell/1', '/Install');

    if  ( $^O eq 'MSWin32' && $is_powershell ) {

       my $command = 'powershell';
       my @args = ("\"Get-WmiObject -Class Win32_Group -Filter \\\"domain=\'\$env:computername\'\\\" | Format-List -Property name\"");

       # defines list of local groups on target node
       my $out = file_execute(
                   cmd => $command,
                   args => \@args,
                   env => { ORACLE_SID => $ENV{GA_DB_SID}, ORACLE_HOME => $ENV{GA_DB_ORACLE_HOME} },
                   user => $ENV{GA_INSTALL_USER}
                 );

       file_foreach_line(
                    $out->{stdout},
                    sub {
                        my $line = shift;
                        return 1 if ($line !~ /^Name\s+:\s+(.+)$/i);
                        my $group = $1;
                        push @localgroups, $group;
                        return 1;
                    }
       );


    } else {

       file_foreach_line(
           '/etc/group',
           sub {
               push @localgroups, (split(':', shift))[0];
               return 1;
           }
       );

    }

    return \@localgroups;

}

sub is_powershell {

    return win32_registry_read_key('LMachine/SOFTWARE/microsoft/powershell/1', '/Install');

}

# argument key shoul be an object returned by Win32::TieRegistry->Open
sub get_registry_key_acl {

    my $key = shift;

    #security descriptor
    my $sDesc;
    $key->RegGetKeySecurity(Win32::DACL_SECURITY_INFORMATION(), $sDesc, []);

    #transform binary descriptor to SDDL (Security Descriptor Definitaion Language)
    use constant SDDL_REVISION_1  => 0x1;

    my $ConvertSDToString = Win32::API->new(
        'ADVAPI32',
        'ConvertSecurityDescriptorToStringSecurityDescriptor',
        ['P', 'N', 'N', 'P', 'N'],
        'N',
    );

    my $ptr_strSDDL = pack 'L', 0; # DWORD;
    my $ObjSD = $ConvertSDToString->Call(
        $sDesc,
        SDDL_REVISION_1,
        0xF,
        $ptr_strSDDL,
        0,
    );

    my $stringSDDL = unpack 'p', $ptr_strSDDL;

    # getting ACE (Access Control Entry) list
    my @ACE = $stringSDDL =~ /(\([^()]+\))/g;

    # transform ACE to human readable format
    # http://msdn.microsoft.com/en-us/library/aa374928(v=vs.85).aspx
    my @ACL = map { transform_ace_to_data($_) } @ACE;

    return \@ACL;

}

sub transform_ace_to_data {

    my $ace = shift;
    $ace =~ s/^\(|\)$//g;

    my ($type, $flags, $rights, $account_sid)  = (split(/;/, $ace))[0,1,2,-1];

    my $account_name = WinUtils::SID_ACCOUNTS()->{$account_sid};
    my $domain;
    my $sid_type;

    if ( ! $account_name ) {
        my $SID = sid_text_to_binary($account_sid);
        Win32::LookupAccountSID('', $SID, $account_name, $domain, $sid_type);
        $account_name = "$domain\\$account_name" if ( $domain );
    }

    my $ace_type = WinUtils::ACE_TYPE()->{$type};
    my @ace_flags = map { WinUtils::ACE_FLAGS()->{$_} } unpack('A2', $flags);
    my @ace_rights = map { WinUtils::ACE_RIGHTS()->{$_} } unpack('A2', $rights);


    return {
        account => $account_name,
        type => $ace_type,
        flags => \@ace_flags,
        rights => \@ace_rights
    };
}


sub sid_text_to_binary {

    my($text_sid) = @_;

    my @Values = split(/\-/, $text_sid);
    ($Values[0] eq "S") or return;
    return pack("CCnnnV" . ($#Values-2), $Values[1], $#Values-2, 0, 0, $Values[2], @Values[3..$#Values]);

}

=item B<parse_ps_table_output>

parses STDOUT output, formated by Format-Table -Property {Proprties_list_separated_by_coma}
arguments:
    reference to scalar containing STDOUT text
    reference to list of properties

=cut

sub parse_ps_table_output {

    my $file= shift;
    my $properties = shift;

    my $re_string = join('.*?', @{$properties});
    my $re = qr/$re_string/;

    my $first_el_length = length($properties->[0]);

    my @lines;
    file_foreach_line(
        $file,
        sub {
            my $line = shift;
            if ( !$line || $line =~ /^\-{$first_el_length}/ || $line =~ /^\s*$/ || $line =~ /$re/) {
                return 1;
            } else {
                my %item;
                @item{@{$properties}} = map { trim($_) } split(/[\x20\x00]+/, $line, scalar @{$properties});
                push @lines, \%item;
            }
            return 1;
        }
    );

    return @lines;
}


#means output should have header like this '------- --- -------...'
sub win32_parse_fixed_output {
    my $file = shift;

    my $re;
    my @columns;
    my @lines;

    file_foreach_line(
        $file,
        sub {
            my $line = shift;
            $line =~ s/^\s+//;

            return 1 if ( !$line || $line =~ /^\s*$/ );

            if ( $line =~ /^\-+/ ) {
                my $space_size = length($1) if ($line =~ /^\-+(\x20+)\-+/);

                @columns = split(/[\x20]{$space_size}/, $line);
                @columns = map { length($_) } @columns;
                my $re_string = '^';
                $re_string .= sprintf("(.{\%d})\\s{$space_size}"x($#columns), @columns);
                $re_string .= "(.{$columns[-1]})\$";

                $re = qr/$re_string/;
                return 1;
            }
            if ( $re && (my @matches = $line =~ $re ) ) {
                @matches = map { trim($_) } @matches;
                push @lines, \@matches;
            }
            return 1;
        }
    );

    return @lines;
}

sub fix_arguments_list {

    my $args = shift;
    foreach my $arg ( @{$args} ) {
        if ( $arg =~ /\s+/ ) {
            $arg = "\"$arg\"";
        }
    }
}

1;
