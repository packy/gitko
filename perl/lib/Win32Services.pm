package Win32Services;
###########################################################################
#
# Win32 helper funcs
#
###########################################################################
use strict;
use Data::Dumper;
use FindBin qw($Bin);
use lib "$Bin/lib";
use FileUtils;
############################################################################
#
# Constants
#
############################################################################
my @WIN32_SERVICE_START = (
    'boot',         # 0
    'system',       # 1
    'auto',         # 2
    'demand',       # 3
    'disabled',     # 4
);
###########################################################################
#
# private subs
#
###########################################################################
sub __win32_sc_exe {

    my (%args) = @_;

    return FileUtils::file_execute(
        %args,
        cmd => 'sc.exe',
    );
}

sub __win32_sc_query_parser {

    my ($line, $services) = @_;
	#
	# Win2000 enumdepend output
	#
	if ($line =~ /Enum:\s+entriesRead\s+=\s+(\d+)/i) {

		my $total_deps = $1;
		if ($total_deps == 0) {

			# stop calling
			return 0;

		} else {

			push @$services, {SERVICE_NAME => '',
					  DISPLAY_NAME => '',
					  TYPE         => '',
					  STATE        => '',
		};
		return 1;
		}
	}

    if (!$line) {
        #
        # new enrty
        #
        push @$services, {SERVICE_NAME => '',
                          DISPLAY_NAME => '',
                          TYPE         => '',
                          STATE        => '',
        };
        return 1;
    }

    if ($line =~ /^\s*(\S+)\s*\:\s*(.*)\s*$/) {

        my ($key, $value) = (uc($1), $2);

        if ($key eq 'STATE') {

            my ($code, $text) = split(/\s+/, $value, 2);

            $services->[-1]->{STATE} = $code;
            $services->[-1]->{STATE_TEXT} = $text;

        } else {

            $services->[-1]->{$key} = $value;
        }

    } elsif ($line =~ /^\s+\(\w+\)\s*$/) {
        #
        #   (STOPPABLE, NOT_PAUSABLE, ACCEPTS_SHUTDOWN)
        #
        $services->[-1]->{STATE_FLAGS} = [split /\,\s*/, $line];
    }

    return 1;
}
##############################################################################
#
# interface
#
##############################################################################
sub win32_service_query {
    #
    # optional
    #
    my $service = shift || '';

    my $result = __win32_sc_exe(
        args => ['query' . ($service ? " \"$service\"" : '')],
        continue_on_error => 1,
    );

    if ($result->{exitcode}) {

        if ($service && defined FileUtils::file_find_first_line($result->{stdout},
                        qr/The specified service does not exist as an installed service/i)) {
            #
            # No such service, ignore.
            #
            return [];
        }

    }
	#
	# try to process anyway
	#
    my @services = ();

    FileUtils::file_foreach_line($result->{stdout}, \&__win32_sc_query_parser, \@services);
    # FIX: remove empty entries
    @services = grep { $_->{SERVICE_NAME} } @services;

	if (scalar(@services) == 0) {

		FileUtils::log("cannot find service [$service]") if $service;
	}

    return \@services;
}

sub win32_service_status {

    my ($service, $status) = @_;

    my $ret = win32_service_query($service);

    if ($ret) {

        map { $status->{$_} = $ret->[0]->{$_} } (keys %{$ret->[0]});

        return 0;
    }
    #
    # service $service does not exist
    #
    return 1;
}

sub win32_service_is_running {

    my $service = shift;

    my %status = ();
    win32_service_status($service, \%status);

    return $status{STATE} == 4;
}

sub win32_service_is_stopped {

    my $service = shift;

    my %status = ();
    win32_service_status($service, \%status);

    return $status{STATE} == 1;
}

sub win32_service_start {

    my $service = shift;

    __win32_sc_exe(
        args => ["start \"$service\""],
        continue_on_error => 1,
    );
    #
    # wait for start. 60 tries, 1 sec interval.
    #
    if (FileUtils::retry_until(
        MAX_TRIES   => 60,
        INTERVAL    => 1,
        CALLBACK    => \&win32_service_is_running,
        CALLBACK_ARG    => $service
    ) == 0) {

        FileUtils::fatal("Start service failed.");
    }
}

sub win32_service_stop {

    my $service = shift;

    my $res = __win32_sc_exe(
        args => ["stop \"$service\""],
        continue_on_error => 1,
    );

    if (defined FileUtils::file_find_first_line($res->{stdout},
                qr/A stop control has been sent to a service that other running services are dependent on/)) {

        FileUtils::fatal("Service dependency detected.");
    }
    #
    # wait for stop
    #
    if (FileUtils::retry_until(
        MAX_TRIES   => 60,
        INTERVAL    => 1,
        CALLBACK    => \&win32_service_is_stopped,
        CALLBACK_ARG    => $service
    ) == 0) {

        FileUtils::fatal("Stop service failed.");
    }
}

sub win32_service_set_start {

    my ($service, $state) = @_;

    return __win32_sc_exe(
        args    => ["config \"$service\" start= $WIN32_SERVICE_START[$state]"],
        expect  => qr/^\[SC\] ChangeServiceConfig SUCCESS$/,
    );
}

sub win32_service_disable_autostart {

    my $service = shift;

    return win32_service_set_start($service, 4);
}

sub win32_service_enable_autostart {

    my $service = shift;

    return win32_service_set_start($service, 2);
}

sub win32_service_config {

    my $service = shift;

    my $ret = __win32_sc_exe(
        args => ["qc \"$service\" 1024"],
        continue_on_error => 1,
    );

    my $success;
    my $current;
    my $doesntexist;

    my $callback = sub {

        my ($line, $config) = @_;

        return 1 unless $line;

        if ($line =~ /The specified service does not exist as an installed service/i) {

            $doesntexist = 1;
            return 0;

        } elsif ($line =~ /The data area passed to a system call is too small\./i) {
            #
            # MS bug
            #
            $doesntexist = 1;
            return 0;

        } elsif ($line =~ /ServiceConfig\s+SUCCESS/i) {

            $success = 1;
            return 1;
        }

        $line =~ s/^\s+//;

        my ($name, $value) = split(/\s*\:\s*/, $line, 2);
        $current = $name if $name;

        if ($value) {
            # leave the leading number only
            $value =~ s/^(\d+)\s+.*$/$1/;
        }

        if ($current eq 'BINARY_PATH_NAME') {
            # split the commandline args
            my @PARSED = ();

            while ($value) {

                if ($value =~ s/^\"(.+?)\"\s*// ||
                    $value =~ s/^(\S+)\s*//) {

                    push @PARSED, $1;

                } else {

                    FileUtils::fatal("parser error.");
                }
            }

            $value = shift @PARSED;
            $config->{BINARY_PATH_ARGS} = \@PARSED;

        } elsif ($current eq 'DEPENDENCIES') {
            #
            # trim
            #
            if ($value) {

                $value =~ s/\s+$//;
                $value =~ s/^\s+//;
            }

            push(@{$config->{$current}}, $value) if $value;
            return 1;
        }

        $config->{$current} = $value;

        return 1;
    };

    my %config = ();
    FileUtils::file_foreach_line($ret->{stdout}, $callback, \%config);

    if ($doesntexist) {
        #
        # non-existing service, ignore
        #
        FileUtils::log("Service [$service] doesn't exist, ignoring...");
        return;

    } elsif (!defined $success) {

        FileUtils::fatal("Command failed.");

    } else {

        return \%config;
    }
}
#######################################################################
#
# call $callback->($service, $user_data) for each service being stopped
#
#######################################################################
sub win32_service_stop_ext {

    my ($service, $callback, $user_data) = @_;

    if (!$service) {

        FileUtils::log("service name empty, nothing to do.");
        return;
    }
    #
    # Fetch the status again in case it changed due to service deps.
    #
    my %status = ();

    if (win32_service_status($service, \%status)) {
        #
        # No such service, nothing to do.
        #
        FileUtils::log("Service [$service] not found, nothing to do.");
        return;
    }

    if ($status{STATE} == 1) { # stopped
        #
        # Service already down, nothing to do
        #
        FileUtils::log("Service [$service] already stopped, nothing to do.");
        return;

    } elsif ($status{STATE} == 3) { # stop pending
        #
        # Just wait until it goes down
        #
        if (FileUtils::retry_until(
            MAX_TRIES   => 60,
            INTERVAL    => 1,
            CALLBACK    => \&win32_service_is_stopped,
            CALLBACK_ARG    => $service
        ) == 0) {

            FileUtils::fatal("state poll failed.");
        }

        return;

    } elsif ($status{STATE} == 2) { # start pending
        #
        # Wait until it's up to stop it.
        #
        if (FileUtils::retry_until(
            MAX_TRIES   => 60,
            INTERVAL    => 1,
            CALLBACK    => \&win32_service_is_running,
            CALLBACK_ARG    => $service
        ) == 0) {

            FileUtils::fatal("state poll failed.");
        }

    } elsif ($status{STATE} != 4) { # running

        FileUtils::log("Unexpected service status [$status{STATE}], will not manage.");
        return;
    }
    #
    # Find service stopping dependencies
    #
    my $deps = win32_service_stop_deps($service);

    if (defined $deps) {
        #
        # stop these as well
        #
        foreach my $dep (@$deps) {

            win32_service_stop_ext($dep->{SERVICE_NAME}, $callback, $user_data);
        }
    }
    #
    # Stop serivce
    #
    FileUtils::log("Shutting down service [$service]...");
    win32_service_stop($service);
    #
    # Call $callback if set
    #
    if (defined $callback) {

        $callback->($service, $user_data);
    }
}

sub win32_service_start_ext {

    my ($service, $callback, $user_data) = @_;

    if (!$service) {

        FileUtils::log("service name empty, nothing to do.");
        return;
    }
    #
    # Fetch the status again in case it changed due to service deps.
    #
    my %status = ();

    if (win32_service_status($service, \%status)) {
        #
        # No such service, nothing to do.
        #
        FileUtils::log("Service [$service] not found, nothing to do.");
        return;
    }

    if ($status{STATE} == 4) { # running
        #
        # Service already up, nothing to do
        #
        FileUtils::log("Service [$service] already started, nothing to do.");
        return;

    } elsif ($status{STATE} == 2) { # start pending
        #
        # Just wait until it's up
        #
        if (FileUtils::retry_until(
            MAX_TRIES   => 60,
            INTERVAL    => 1,
            CALLBACK    => \&win32_service_is_running,
            CALLBACK_ARG    => $service
        ) == 0) {

            FileUtils::fatal("state poll failed.");
        }

        return;

    } elsif ($status{STATE} == 3) { # stop pending
        #
        # Wait until it's down
        #
        if (FileUtils::retry_until(
            MAX_TRIES   => 60,
            INTERVAL    => 1,
            CALLBACK    => \&win32_service_is_stopped,
            CALLBACK_ARG    => $service
        ) == 0) {

            FileUtils::fatal("state poll failed.");
        }

    } elsif ($status{STATE} != 1) { # stopped
        #
        # Unexpected status
        #
        FileUtils::log("Unexpected service status [$status{STATE}], will not manage.");
        return;
    }
    #
    # Check for deps
    #
    my $conf = win32_service_config($service);

    if (defined $conf) {
        #
        # start all dependent services
        #
        foreach my $dep (@{$conf->{DEPENDENCIES}}) {

            win32_service_start_ext($dep);
        }
    }
    #
    # start
    #
    FileUtils::log("Starting up service [$service]...");
    win32_service_start($service);
    #
    # callback?
    #
    if (defined $callback) {

        $callback->($service, $user_data);
    }
}

sub win32_service_stop_deps {

    my $service = shift;
    my $buffer  = 1024;
    my $ret;

    while (1) {

        $ret = __win32_sc_exe(
            args => ["enumdepend \"$service\" $buffer"],
			continue_on_error => 1,
        );

        if (FileUtils::file_find_first_line($ret->{stdout},
            qr/\[SC\] EnumDependentServices\: more data\, need \d+ bytes/i)) {

            $buffer += 1024;

        } else {

            last;
        }
    }

    my $total_deps;
    my @services = ();

    FileUtils::file_foreach_line($ret->{stdout}, \&__win32_sc_query_parser, \@services);
    # FIX: remove empty entries
    @services = grep { $_->{SERVICE_NAME} } @services;

    FileUtils::log("Found deps: " . Dumper(\@services));

    return \@services;
}

1;

__END__
