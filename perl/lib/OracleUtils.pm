package OracleUtils;
use strict;
use warnings;

use File::Spec;
use Hash::Util;
use IO::File;

use FindBin qw($Bin);
use lib "$Bin/lib";
use FileUtils;
###############################################################################
#
# run sqlplus
#
###############################################################################

sub get_oracle_output_lines {
    my $query = shift;
    push @_, ('feedback_off', 1);

    my $result = OracleUtils::oracle_run_sql($query, @_);

    my @lines;
    FileUtils::file_foreach_line(
        $result->{stdout},
        sub {
            push @lines, $_[0];

            return 1;
        }
    );

    return @lines;
}

sub oracle_run_sql
{
    my ($query, @args) = @_;

    # Merge with defaults
    my %flags = (
        oracle_home => $ENV{GA_DB_ORACLE_HOME},
        oracle_sid  => $ENV{GA_DB_SID},
        user        => $ENV{GA_INSTALL_USER},
        @args
    );

    Hash::Util::lock_keys(%flags);

	FileUtils::fatal("oracle_run_sql: OH not set!")
		unless exists $flags{oracle_home};


    #
    # cannot have both
    #
	FileUtils::fatal("oracle_run_sql: both sid and tnsalias are set!")
		if
		   	exists $flags{oracle_sid} && defined $flags{oracle_sid}
			&& exists $flags{tnsalias} && defined $flags{tnsalias};


	FileUtils::fatal("oracle_run_sql: sid or tnsalias should be set!")
		if !exists $flags{oracle_sid} && !exists $flags{tnsalias};


	FileUtils::fatal( "oracle_run_sql: tnsalias used, but no username/password supplied" )
		if
			exists $flags{tnsalias} &&
			( !exists $flags{sysuser} || !exists $flags{syspassword} );


	FileUtils::fatal("TNSALIAS cannot be empty.")
		if exists $flags{tnsalias} && !$flags{tnsalias};


	FileUtils::fatal("SYSUSER cannot be empty.")
		if exists $flags{sysuser} && !$flags{sysuser};


    my $connect_str = '';

    if( exists $flags{tnsalias} ) {
        $connect_str = $flags{sysuser} . '/' . $flags{syspassword} . '@' . $flags{tnsalias};
    } elsif( exists $flags{sysuser} && exists $flags{syspassword} ) {
        $connect_str = $flags{sysuser} . '/' . $flags{syspassword};
	} else {
		$connect_str = '/';
	}

	if (exists $flags{as_sysasm} && $flags{as_sysasm}) {

		$connect_str .= ' AS SYSASM';

	} elsif (exists $flags{nosysdba} && $flags{nosysdba}) {

		# nothing. Left blank on purpose.

	} else {

		$connect_str .= ' AS SYSDBA';
	}

    my $error_policy;
    my $feedback;

    $feedback = (exists $flags{feedback_off} && $flags{feedback_off}) ? "set feedback off;\n" : '';

    if (exists $flags{continue_on_sqlerror} && $flags{continue_on_sqlerror}) {

		$error_policy = 'whenever sqlerror continue;';

    } else {

		$error_policy = 'whenever sqlerror exit failure;';
    }

    my $spool = '';

    if (exists $flags{spool}) {

       	$spool = 'SPOOL ' . $flags{spool};
    }

	my $DEFAULT =<<DEF;
$feedback
$error_policy
whenever oserror exit failure;
DEF

	if (!exists $flags{noopts} || !$flags{noopts}) {

		$DEFAULT .=<<DEF;
SET pagesize 0;
SET linesize 32767
SET trimout on
SET colsep '|'
SET tab off
$spool
DEF
	}

    my $sql =<<SQL;
$DEFAULT
$query
EXIT;
SQL

    my ($fh, $queryfile) = FileUtils::file_temp_filename(
       	'sqlplus_XXXXXX',
       	SUFFIX => '.sql',
        DIR    => exists $flags{DIR} && $flags{DIR}
            ? $flags{DIR} : undef,
       	UNLINK => 0
   	);

   	$fh->print($sql);
   	$fh->close();

    FileUtils::log("will execute query [$sql]");

    if (exists $flags{user} && defined $flags{user}) {

        FileUtils::file_chown($flags{user}, $queryfile);
    }

    my $SID =
      (exists $flags{oracle_sid}) ? $flags{oracle_sid} : $flags{tnsalias};

    my %env = (ORACLE_HOME => $flags{oracle_home});

    if (exists $flags{oracle_sid})
    {
        $env{ORACLE_SID} = $flags{oracle_sid};
    }

    my $ret = FileUtils::file_execute(
        cmd  => $flags{oracle_home} . '/bin/sqlplus',
        args => ["-L -S \"$connect_str\" \@$queryfile"],
        %flags,
        env => \%env,
    );

    return $ret;
}

sub oracle_run_sqlplus_remote
{
    my ($query, %flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $error_policy;

    if (exists $flags{continue_on_sqlerror} && $flags{continue_on_sqlerror})
    {
        $error_policy = 'whenever sqlerror continue;';
    }
    else
    {
        $error_policy = 'whenever sqlerror exit failure;';
    }

    my $sql = <<SQLPLUS;
$error_policy
whenever oserror exit failure;
SET pagesize 0;
SET linesize 32767
SET trimout on
SET colsep '|'
SET tab on
$query
EXIT;
SQLPLUS

    my $tempfile = FileUtils::file_temp_filename(
        "sqlplus_XXXXXX",
        SUFFIX => '.sql',
        UNLINK => 0
    );

    FileUtils::file_append($tempfile, $sql);
    FileUtils::file_chown($flags{user}, $tempfile) if defined $flags{user};

    my (undef, undef, $filename) = File::Spec->splitpath($tempfile);

    FileUtils::file_copy_remote(
        [
            {
                NODE      => $flags{rhost},
                OWNER     => $flags{ruser},
                GROUP     => exists $flags{rgroup} ? $flags{rgroup} : undef,
                SRC_FILE  => $tempfile,
                DEST_FILE => "/tmp/$filename",
                SSH_USER  => $flags{user} ? $flags{user} : $flags{ruser},
            }
        ]
    );

    FileUtils::log("Will run [$query] as [" .
	exists $flags{ruser} && defined $flags{ruser} ? $flags{ruser} : $flags{user} .
    "]");

    my $connect_str = '/';

    $connect_str .= exists $flags{as_sysasm} && $flags{as_sysasm}
        ? ' AS SYSASM'
        : ' AS SYSDBA';

    return FileUtils::file_execute(
        cmd  => $flags{oracle_home} . "/bin/sqlplus -L -S \"$connect_str\" \@/tmp/$filename",
        renv => {
            ORACLE_HOME => $flags{oracle_home},
            ORACLE_SID  => $flags{oracle_sid},
        },
        %flags
    );
}

sub is_instance_running {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    if (exists $args{crs_home}) {

        return oracle_crs_status(%args);

    } elsif (exists $args{grid_home}) {

        return oracle_single_grid_status(%args);

    } else {

        my $ret = oracle_run_sql(
            'SELECT SHUTDOWN_PENDING, DATABASE_STATUS FROM v$instance;',
            continue_on_sqlerror => 1,
            %args,
        );

        if (defined FileUtils::file_find_first_line($ret->{stdout}, qr/\|ACTIVE/i)) {

            return 1;
        }

        if (defined FileUtils::file_find_first_line(
            $ret->{stdout},
            qr/ORA\-01034\: ORACLE not available/i
        )) {

            return 0;
        }

        FileUtils::log("WARNING: unknown state!");
    }

    return 0;
}

sub get_vparameter
{

    my ($param, %args) = @_;
    Hash::Util::lock_keys(%args);

    $param = lc $param;

    my $ret =
      oracle_run_sql("select value from v\$parameter where name='$param';",
        %args,);

    my $val;

    FileUtils::file_foreach_line($ret->{stdout},
        sub {$val = shift; return 0;});

    if (!defined $val) {

        FileUtils::fatal("cannot fetch parameter [$param]");
    }

    return $val;
}

sub get_vdatabase
{

    my ($param, %args) = @_;
    Hash::Util::lock_keys(%args);

    $param = lc $param;

    my $ret = oracle_run_sql(
        "select $param from v\$database;",
        %args
    );

    my $val;

    FileUtils::file_foreach_line($ret->{stdout},
        sub {$val = shift; return 0;}
    );

    if (!defined $val || !$val) {

        FileUtils::fatal("cannot fetch parameter [$param]");
    }

    if (lc $val eq 'no rows selected') {

        return '';
    }

    return $val;
}

sub get_voption
{

    my ($param, %args) = @_;
    Hash::Util::lock_keys(%args);

    my $ret =
      oracle_run_sql("select value from v\$option where parameter='$param';",
        %args,);

    my $val;

    FileUtils::file_foreach_line($ret->{stdout},
        sub {$val = shift; return 0;});

    if (!defined $val || !$val)
    {
        FileUtils::fatal("cannot fetch option [$param]");
    }

    return $val;
}

sub fetch_as_arrayref
{

    my $result = shift;

    my $callback = sub {

        my ($line, $arr) = @_;

        if ($line =~ /\w+ rows? selected/)
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

sub oratab_as_hash
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $oratab = oracle_get_oratab(%args);

    return FileUtils::file_read_as_hash(
        file => $oratab,
        separator => qr/\:/,
        %args,
    );
}

sub find_crs_home {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $crs_home;
    #
    # 1. First try $oh/srvm/admin/getcrshome
    #
    if (exists $args{oracle_home}) {

        if (FileUtils::file_test(
            FILE => "$args{oracle_home}/srvm/admin/getcrshome",
            TEST => '-x',
            NODE => exists $args{rhost} && $args{rhost} ? $args{rhost} : undef,
            LOCAL_USER => $args{user},
        )) {

            my $ret = FileUtils::file_execute(
                cmd => "$args{oracle_home}/srvm/admin/getcrshome",
            );

            FileUtils::file_foreach_line($ret->{stdout}, sub { $crs_home = shift; return 0; });

            return $crs_home if defined $crs_home;
        }
    }
    #
    # 2. Then try the inventory
    #
    my $inv = oracle_inventory_as_hash(%args);

    foreach my $oh (keys %$inv) {

        return $oh
            if exists $inv->{$oh}->{CRS} && lc $inv->{$oh}->{CRS} eq 'true';
    }
    #
    # 3. Last, grep init_crsd
    #
    my $init_crsd;

    if (exists $args{version}) {

        $init_crsd = ($args{version} eq '11.2')
            ? __find_init_crsd_11_2(%args)
            : __find_init_crsd_10_2(%args);

    } else {

        $init_crsd = __find_init_crsd_11_2(%args) || __find_init_crsd_10_2(%args);
    }

    return undef unless defined $init_crsd;

    my $file;

    if (exists $args{rhost} && defined $args{rhost}) {

        my $result = FileUtils::file_execute(
            %args,
            cmd  => 'cat',
            args => [$init_crsd],
        );

        $file = $result->{stdout};

    } else {

        $file = $init_crsd;
    }

    my $line = FileUtils::file_find_first_line($file, qr/ORA_CRS_HOME\=/);

    FileUtils::fatal("cannot determine ORA_CRS_HOME.")
      unless defined $line;

    (undef, $crs_home) = split(/\=\s*/, $line, 2);

    FileUtils::fatal("cannot determine ORA_CRS_HOME.")
      unless defined $crs_home;

    FileUtils::log("Found crs home [$crs_home]");

    return $crs_home;
}

sub oracle_instance_startup_if_not_running {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $is_running = is_instance_running(%flags);

    if (!$is_running)
    {
        oracle_instance_startup(%flags);
    }

    return $is_running;
}

sub oracle_instance_shutdown_if_running
{

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $is_running = is_instance_running(%flags);

    if ($is_running)
    {
        oracle_instance_shutdown(%flags);
    }
}

sub oracle_instance_startup {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    if (exists $flags{grid_home} && defined $flags{grid_home}) {

        return oracle_single_grid_startup(%flags);

    } elsif (exists $flags{crs_home} && defined $flags{crs_home}) {

        return oracle_crs_startup(%flags);

    } else {

        return oracle_single_startup(%flags);
    }
}

sub oracle_single_startup
{
    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $query = 'STARTUP';

    if (exists $flags{option})
    {

        $query .= ' ' . $flags{option};
    }

    $query .= ';';

    return oracle_run_sql($query, %flags,
        expect => qr/Oracle instance started\./i,);
}

sub oracle_single_grid_startup {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $sid = lc $flags{oracle_sid};

    return FileUtils::file_execute(
        cmd  => $flags{grid_home} . '/bin/srvctl',
        args => ["start database -d $sid"],
        user => $flags{user},
    );
}

sub oracle_crs_startup {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    return FileUtils::file_execute(
        cmd  => $flags{grid_home} . '/bin/srvctl',
        args => ["start database -d $flags{dbname} -i $flags{oracle_sid}"],
        user => $flags{user},
    );
}

sub oracle_single_grid_status {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $sid = lc $flags{oracle_sid};

    my $ret = FileUtils::file_execute(
        cmd  => $flags{grid_home} . '/bin/srvctl',
        args => ["status database -d $sid"],
        user => $flags{user},
    );

    return defined FileUtils::file_find_first_line(
        $ret->{stdout},
        qr/is running/i,
    );
}

sub oracle_crs_status {

    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $ret = FileUtils::file_execute(
        cmd  => $flags{crs_home} . '/bin/srvctl',
        args => ["status instance -d $flags{dbname} -i $flags{oracle_sid}"],
        user => $flags{user},
    );

    return defined FileUtils::file_find_first_line(
        $ret->{stdout},
        qr/is running/i,
    );
}

sub oracle_single_shutdown
{
    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    return oracle_run_sql(
        'SHUTDOWN IMMEDIATE;',
        %flags,
        expect               => qr/Oracle instance shut down\./i,
        continue_on_sqlerror => 1,
    );
}

sub oracle_single_grid_shutdown
{
    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    my $sid = lc $flags{oracle_sid};

    return FileUtils::file_execute(
        cmd  => $flags{grid_home} . '/bin/srvctl',
        args => ["stop database -d $sid"],
        %flags,
    );
}

sub oracle_instance_shutdown
{
    my (%flags) = @_;
    Hash::Util::lock_keys(%flags);

    return (exists $flags{grid_home} && defined $flags{grid_home})
      ? oracle_single_grid_shutdown(%flags)
      : oracle_single_shutdown(%flags);
}

sub oracle_crs_stat
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $crs_home = $args{crs_home};
    if (!defined $crs_home)
    {
        FileUtils::fatal("No CRS_OH found");
    }

    return FileUtils::file_execute(
        cmd => $crs_home . '/bin/crs_stat',
        %args
    );
}

sub oracle_get_oratab
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    foreach my $oratab ('/etc/oratab', '/var/opt/oracle/oratab') {

        if (FileUtils::file_test(
            FILE => $oratab,
            TEST => '-f',
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => $args{user})) {

            return $oratab;
        }
    }

    FileUtils::fatal("cannot find oratab.")
}

sub oracle_find_inventory_ptr {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $invptr;
    #
    # First look in $OH/oraInst.loc
    #
    if (exists $args{oracle_home}) {

        $invptr = File::Spec->catfile($args{oracle_home}, 'oraInst.loc');
    #
    # then in /etc
    #
    } else {

        $invptr = File::Spec->catfile('etc', 'oraInst.loc');
    }

    return $invptr;
}

sub oracle_find_inventory {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $invptr = oracle_find_inventory_ptr(%args);
    #
    # support remote exec
    #
    my $result = FileUtils::file_execute(
        %args,
        cmd => 'cat',
        args => [$invptr],
    );

    my $oraInv;

    my $callback = sub {

        my $line = shift;

        if ($line =~ /^inventory_loc\=(.*)$/) {

            $oraInv = $1;
            return 0;
        }

        return 1;
    };

    FileUtils::file_foreach_line($result->{stdout}, $callback);

    FileUtils::log("Found inventory location [$oraInv]");

    return $oraInv;
}

sub oracle_inventory_as_hash {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $inv = oracle_find_inventory(%args);
    #
    # support remote exec
    #
    my $result = FileUtils::file_execute(
        %args,
        cmd => 'cat',
        args => ["$inv/ContentsXML/inventory.xml"],
    );
    #
    # Typical entry:
    # <HOME NAME="CLONED" LOC="/app/oracle/EMPTYSW/product/10.2.0.4" TYPE="O" IDX="5" REMOVED="T"/>
    #
    my $callback = sub {

        my ($line, $hash) = @_;

        if ($line =~ /^\<HOME /) {
            #
            # remove quotes
            #
            $line =~ s/\"//g;
	    #
            # remove tags
            #
            $line =~ s/^\s*\<//;
            $line =~ s/\>\s*//;
            #
            # split by space
            #
            my @pairs = split(/\s+/, $line);
            my $entry = {};

            foreach my $pair (@pairs) {

                my ($key, $value) = split(/\=/, $pair, 2);
                $entry->{$key} = $value;
            }

            $hash->{$entry->{LOC}} = $entry unless exists $entry->{REMOVED};
        }

        return 1;
    };

    my %hash = ();

    FileUtils::file_foreach_line($result->{stdout}, $callback, \%hash);

    return \%hash;
}

sub oracle_home_is_rac {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $out = FileUtils::file_execute(
        cmd   => 'ar',
        args  => ["-t $args{oracle_home}/rdbms/lib/libknlopt.a"],
    );

    return
        defined FileUtils::file_find_first_line($out->{stdout}, qr/kcsm\.o/);
}

sub oracle_base {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $ret = FileUtils::file_execute(
        cmd => File::Spec->rel2abs(
            'bin/orabase',
            $args{oracle_home},
        ),
        user => $args{user},
        env  => {ORACLE_HOME => $args{oracle_home},
        },
    );

    my $oracle_base;

    FileUtils::file_foreach_line(
        $ret->{stdout},
        sub { $oracle_base = shift; return 0; }
    );

    if (!defined $oracle_base) {

        FileUtils::fatal("failed to fetch ORACLE_BASE");
    }

    return $oracle_base;
}
#############################################################################
#
# private
#
#############################################################################
sub __find_init_crsd_10_2
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    foreach my $loc (qw(/etc/init.d/init.crsd /etc/init.crsd))
    {
        if (FileUtils::file_test(
            TEST => '-f',
            FILE => $loc,
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => exists $args{user} ? $args{user} : undef)) {

            return $loc;
        }
    }

    FileUtils::log("Cannot find [init.crsd].");
    return undef;
}

sub __find_init_crsd_11_2
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    foreach my $loc (qw(/etc/init.d/init.ohasd /etc/init.ohasd))
    {
        if (FileUtils::file_test(
            TEST => '-f',
            FILE => $loc,
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => exists $args{user} ? $args{user} : undef)) {

            return $loc;
        }
    }

    FileUtils::log("Cannot find [init.ohasd].");
    return undef;
}

sub get_local_listener_ora {
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my @locations = (
        File::Spec->catfile($args{tns_admin}, 'listener.ora')
    );

    if ( exists $ENV{GA_CRS_ORACLE_HOME} && $ENV{GA_CRS_ORACLE_HOME} ) {
        push @locations,
            File::Spec->catfile($ENV{GA_CRS_ORACLE_HOME}, 'network', 'admin', 'listener.ora');
    }

    push @locations,
        File::Spec->catfile($args{oracle_home}, 'network', 'admin', 'listener.ora');

    foreach my $loc (@locations)
    {
        if (FileUtils::file_test(
            TEST => '-f',
            FILE => $loc,
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => exists $args{user} ? $args{user} : undef)) {

            return $loc;
        }
    }

    FileUtils::log("Cannot find [listener.ora].");
    return undef;
}

sub get_local_cman_ora {
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my @locations = (
        File::Spec->catfile($args{tns_admin}, 'cman.ora')
    );

    if ( exists $ENV{GA_CRS_ORACLE_HOME} && $ENV{GA_CRS_ORACLE_HOME} ) {
        push @locations,
            File::Spec->catfile($ENV{GA_CRS_ORACLE_HOME}, 'network', 'admin', 'cman.ora');
    }

    push @locations,
        File::Spec->catfile($args{oracle_home}, 'network', 'admin', 'cman.ora');

    foreach my $loc (@locations)
    {
        if (FileUtils::file_test(
            TEST => '-f',
            FILE => $loc,
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => exists $args{user} ? $args{user} : undef)) {

            return $loc;
        }
    }

    FileUtils::log("Cannot find [cman.ora].");
    return undef;
}

sub get_local_sqlnet_ora {
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my @locations = (
        File::Spec->catfile($args{tns_admin}, 'sqlnet.ora')
    );

    if ( exists $ENV{GA_CRS_ORACLE_HOME} && $ENV{GA_CRS_ORACLE_HOME} ) {
        push @locations,
            File::Spec->catfile($ENV{GA_CRS_ORACLE_HOME}, 'network', 'admin', 'sqlnet.ora');
    }

    push @locations,
        File::Spec->catfile($args{oracle_home}, 'network', 'admin', 'sqlnet.ora');

    foreach my $loc (@locations)
    {
        if (FileUtils::file_test(
            TEST => '-f',
            FILE => $loc,
            NODE => exists $args{rhost} ? $args{rhost} : undef,
            USER => undef,
            LOCAL_USER => exists $args{user} ? $args{user} : undef)) {

            return $loc;
        }
    }

    FileUtils::log("Cannot find [sqlnet.ora].");
    return undef;
}

sub get_local_dba_group {

    my $dba_group = 'dba';
    my %args = @_;
    my $config = File::Spec->catfile($args{oracle_home}, 'rdmbs', 'lib', 'config.c');

    if ( -e $config ) {
        File::file_foreach_line(
            $config,
            sub {
                my $line = shift;
                if ( $line =~ /^\#define\s+SS_DBA_GRP\s+(\w+)$/ ) {
                    $dba_group = $1;
                    return 0;
                }
                return 1;
            }
        );
    }

    return $dba_group;

}

sub get_listener_names_by_sid {
    my (%args) = @_;
    Hash::Util::lock_keys(%args);
	my $listener_names_with_path_array;
	my $listener_names_array;
	my $oracle_sid = ($args{oracle_sid}) ? $args{oracle_sid} : $ENV{GA_DB_SID};
	my $cmd = 'ps -ef | grep tnslsnr | grep -v grep | awk -F" " \'{print $8" "$9}\'';
	my $ret = FileUtils::file_execute(
        cmd  => $cmd,
        args => [],
		continue_on_error => 1,
        %args,
    );
	
	FileUtils::file_foreach_line(
		$ret->{stdout},
		sub {
			my $listener_name = shift;
			push @{$listener_names_with_path_array}, $listener_name;
			return 1;
		}
	);
	
	if ($listener_names_with_path_array) {
		my $listener_home;
		my $listener_name;
		
		foreach my $path_name (@{$listener_names_with_path_array}) {
			($listener_home, $listener_name) = split(/\s/,$path_name);
			(undef,$listener_home,undef) = File::Spec->splitpath($listener_home);
			my $command = File::Spec->catfile($listener_home, 'lsnrctl');
			$listener_home =~ s/\/bin\/$//;
			my $ret = FileUtils::file_execute(
				cmd  => $command,
				args => ["status $listener_name"],
				env => {ORACLE_HOME => $listener_home},
			);
	
			FileUtils::file_foreach_line(
				$ret->{stdout},
				sub {
					my $instance_string = shift;
					return 1 if ($instance_string !~ /^.+Instance.+$oracle_sid/);
					push @{$listener_names_array}, $listener_name;
					return 0;
				}
			);
			
			
		}
	}
    return $listener_names_array;
}


1;
__END__
