package MssqlUtils;
use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/lib";

use File::Spec;
use Data::Dumper;
use Hash::Util;
use IO::Handle;
use FileUtils;
################################################################################
#
# constants
#
################################################################################
my %CONSTANTS = (

    '2000' => {REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/80/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'osql.exe',
               ARGS    => '-n',
    },

    '2005' => {REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/90/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },

    '2008' => {REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/100/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },

    '2008R2'=>{REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/100/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },

    '2012'  =>{REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/110/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },

    '2014'  =>{REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/120/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },

    'Unknown SQL Server Edition' =>
              {REG_PATH=> 'HKEY_LOCAL_MACHINE/SOFTWARE/Microsoft/Microsoft SQL Server/120/Tools/ClientSetup',
               REG_KEY => 'SQLPath',
               EXE     => 'sqlcmd.exe',
               ARGS    => '-W',
    },
);
################################################################################
#
# generate sql query file
#
################################################################################
sub __osql_generate_script {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

	FileUtils::log("Will run query [$args{query}]");

    my ($fh, $file) = FileUtils::file_temp_filename('query_XXXXXX',
											SUFFIX => '.sql',
								UNLINK => 0);

    $fh->print("use $args{database}\r\nGO\r\n")
        if exists $args{database};

    my $SQL =<<TSQL;
$args{query}
GO
TSQL

    $fh->print($SQL);
    $fh->close();

    return $file;
}

sub find_tools_path {

	my (%args) = @_;
    Hash::Util::lock_keys(%args);

	my $tools_path;
	my $version = exists $args{version} && $args{version} ? $args{version} : undef;
    #
    # Try each 2008, 2005 and 2000 and use the first found.
    #
    if (!defined $version) {

        foreach my $v ('Unknown SQL Server Edition', '2012', '2008R2', '2008', '2005', '2000') {

            if (!exists $CONSTANTS{$v}) {

                FileUtils::fatal("Unsupported sqlserver version.");
            }

            $tools_path = FileUtils::win32_registry_read_key(
                $CONSTANTS{$v}->{REG_PATH},
                $CONSTANTS{$v}->{REG_KEY},
            );

			if ($tools_path) {

				$version = $v;
				last;
			}
        }

    } else {

        if (!exists $CONSTANTS{$version}) {

            FileUtils::fatal("Unsupported sqlserver version.");
        }

        $tools_path = FileUtils::win32_registry_read_key(
            $CONSTANTS{$version}->{REG_PATH},
            $CONSTANTS{$version}->{REG_KEY}
        );
    }

	return ($version, $tools_path);
}
################################################################################
#
# run osql
#
################################################################################
sub osql {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $ifile;

    if (exists $args{query}) {

        $ifile = __osql_generate_script(
            query => $args{query},
        );

    } elsif (exists $args{sqlscript}) {

        $ifile = $args{sqlscript};

    } else {

        FileUtils::fatal("No sql to run.");
    }

	if (-z $ifile) {

		FileUtils::fatal("scriptfile [$ifile] is zero size.");
	}

    my $instname = $args{instance};
    $instname =~ s/\\\(local\)$//;
    my $database = exists $args{database} ? $args{database} : undef;
    my $user     = exists $args{user}     ? $args{user}     : undef;
    my $passwd   = exists $args{passwd}   ? $args{passwd}   : undef;
    my $version  = exists $args{version}  ? $args{version}  : undef;

	my ($version, $tools_path) = find_tools_path(%args);

	if (!$tools_path) {

		FileUtils::fatal("Cannot find tools path");
    }

    my $cmd = File::Spec->catfile(
        $tools_path,
        'Binn',
        $CONSTANTS{$version}->{EXE}
    );
	#
	# -r[0|1] - redirect error messages to stderr
	#
    my @ARGS = ("-w65535 -s\"|\" -r0 -h-1 -i\"$ifile\"");

    if (!exists $args{continue_on_sqlerror} || !$args{continue_on_sqlerror}) {
		#
		# set ERRORLEVEL=%SQLCMDERRORLEVEL%
		#
		push @ARGS, '-b';
	}

    push @ARGS, $CONSTANTS{$version}->{ARGS};
    #
    # DB-level auth
    #
    my $sa        = exists $args{sa}        ? $args{sa}        : undef;
    my $sa_passwd = exists $args{sa_passwd} ? $args{sa_passwd} : '';

    if (defined $sa) {
		#
		# DB-level auth
		#
        push @ARGS, "-U $sa -P $sa_passwd";

    } else {
		#
		# windows authentication
		#
		push @ARGS, '-E';
	}

        my $conn_str = '';
	#
	# rhost overrides instance
	#
	if (exists $args{rhost} && $args{rhost}) {

		$conn_str .= "$args{rhost}\\";
	}

	$conn_str .= $instname;

	if (exists $args{port} && $args{port}) {

		$conn_str = 'tcp:' . $conn_str;
		$conn_str .= ",$args{port}";
	}

	push @ARGS, "-S $conn_str";

        if (exists $args{extra} && $args{extra}) {

            push @ARGS, '-I';
        }

    return FileUtils::file_execute(
        %args,
        user   => $user,
        passwd => $passwd,
        cmd    => qq("$cmd"),
        args   => \@ARGS,
    );
}
################################################################################
#
# return array (each row) of arrays (each result)
#
################################################################################
sub fetch_as_arrayref
{
    my $result = shift;

    my $callback = sub {

        my ($line, $arr) = @_;

        if ($line =~ /\w+ rows? affected/)
        {

            return 0;
        }

        $line =~ s/^\s*//;
        $line =~ s/\s*$//;

        return 1 unless $line;

        my @record = split(/\s*\|\s*/, $line);

        push @$arr, \@record;

        return 1;
    };

    my @ret = ();
    FileUtils::file_foreach_line($result->{stdout}, $callback, \@ret);

    return \@ret;
}
###################################################################
#
# Instance scoped subs
#
###################################################################
sub mssql_instance_id {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $inst;

    if (exists $args{instance} && $args{instance}) {

        (undef, $inst) = split /\\/, $args{instance}, 2;
        $inst = $args{instance} unless $inst;
        $inst = 'MSSQLSERVER' if $inst eq '(local)';

    } else {

        $inst = 'MSSQLSERVER';
    }

    return FileUtils::win32_registry_read_key(
        'HKEY_LOCAL_MACHINE/Software/Microsoft/Microsoft SQL Server/Instance Names/SQL',
        $inst
    );
}

sub datafiles_paths {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my %ret = (
        DefaultData => '',
        DefaultLog  => '',
#        BackupDirectory => '',
    );

    my $inst_id = mssql_instance_id(%args);

    if (!$inst_id) {

        FileUtils::fatal("cannot find instance id in the registry.");
    }

    foreach my $loc (keys %ret) {

        $ret{$loc} = FileUtils::win32_registry_read_key(
            "HKEY_LOCAL_MACHINE/Software/Microsoft/Microsoft SQL Server/$inst_id/MSSQLServer",
            $loc,
        );

        if (!$ret{$loc}) {
            #
            # DefaultData and DefaultLog not set, use SQLDataRoot
            #
            $ret{DefaultData} = $ret{DefaultLog} = FileUtils::win32_registry_read_key(
                "HKEY_LOCAL_MACHINE/Software/Microsoft/Microsoft SQL Server/$inst_id/Setup",
                'SQLDataRoot',
            );

            last;
        }
    }

    foreach my $key (keys %ret) {

        if (!-d "$ret{$key}") {

            FileUtils::fatal("Path [$ret{$key}] doesn't exist.");
        }
    }

    FileUtils::log("Will use datafiles paths " . Dumper(\%ret));

    return \%ret;
}
#####################################################################
#
# Database scoped subs
#
#####################################################################
#####################################################
#
# sysdatabases.status bits
#
#####################################################
use constant {
    #
    # Status bits, some of which can be set by the user with
    # sp_dboption (read only, dbo use only, single user, and so on):
    #
    STATUS_AUTOCLOSE    => 0x000000001, # 1 = autoclose; set with sp_dboption.
    STATUS_BULKCOPY     => 0x000000004, # 4 = select into/bulkcopy; set with sp_dboption.
    STATUS_TRUNCATE     => 0x000000008, # 8 = trunc. log on chkpt; set with sp_dboption.
    STATUS_TORNPAGE     => 0x000000010, # 16 = torn page detection, set with sp_dboption.
    STATUS_LOADING      => 0x000000020, # 32 = loading.
    STATUS_PRERECOVERY  => 0x000000040, # 64 = pre recovery.
    STATUS_RECOVERING   => 0x000000080, # 128 = recovering.
    STATUS_NOTRECOVERED => 0x000000100, # 256 = not recovered.
    STATUS_OFFLINE      => 0x000000200, # 512 = offline; set with sp_dboption.
    STATUS_READONLY     => 0x000000400, # 1024 = read only; set with sp_dboption.
    STATUS_DBOONLY      => 0x000000800, # 2048 = dbo use only; set with sp_dboption.
    STATUS_SINGLEUSER   => 0x000001000, # 4096 = single user; set with sp_dboption.
    STATUS_EMERGENCY    => 0x000008000, # 32768 = emergency mode.
    STATUS_AUTOSHRINK   => 0x000400000, # 4194304 = autoshrink.
    STATUS_CLEANSHUTDOWN=> 0x040000000, # 1073741824 = cleanly shutdown.
};

sub mssqldb_status {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $res = osql(
        query => "SELECT status FROM [master].[dbo].[sysdatabases] WHERE name = '$args{database}'",
        %args,
    );

    my $status;

    FileUtils::file_foreach_line($res->{stdout}, sub { $status = $_[0]; return 0 });

    return $status;
}

sub mssqldb_get_option {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $ret = osql(
        %args,
        query => "EXEC sp_dboption '$args{database}', '$args{option}'",
    );

    my $val;
    my $cb = sub {

        my $line = shift;

        return 1 unless $line;

        (undef, $val) = split /\|/, $line;
        return 0;
    };

    FileUtils::file_foreach_line($ret->{stdout}, $cb);

    if (!defined $val) {

        FileUtils::fatal("failed to fetch option [$args{option}]");
    }

    return $val;
}

1;
__END__
