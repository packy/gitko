#!/app/clarity/perl/bin/perl
use strict;
use warnings;

use Hash::Util;
use Data::Dumper;
use FindBin qw($Bin);
use lib "$Bin/lib";

use GridApp::Archive;
use ComplianceUtils qw/exitcode_to_status/;
use XMLUtils;

use GA::Standards::Result;
use GA::Standards::ResultsCodes ':codes';

$SIG{__WARN__} = sub {FileUtils::fatal("FATAL WARNING: @_");};

my $result = GA::Standards::Result->new();

my $DIR = File::Spec->catdir(
    $ENV{GA_SCRIPT_DIR},
    $ENV{GAC_SUITE}
);

# exclude checks
my %exclude_checks;
if ( $ENV{GAC_EXCLUDED_CHECKS} ) {
    %exclude_checks = map { s/ //g; $_ => 1 } split(/,/, $ENV{GAC_EXCLUDED_CHECKS});
}

sub run_command {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $ret = FileUtils::file_execute(
        %args,
        DIR  => $args{logdir},
        cmd  => $args{execute},
        user => $ENV{GA_INSTALL_USER},
        env  => {ORACLE_HOME => $ENV{GA_DB_ORACLE_HOME},
                 ORACLE_SID  => $ENV{GA_DB_SID},
        },
        continue_on_error   => 1,
    );

    my $status =
      $ret->{exitcode}
      ? COMPLIANCE_RESULT_NON_COMPLIANT
      : COMPLIANCE_RESULT_COMPLIANT;

    # We have only two states for external command: COMPLIANT and NON COMPLIANT
    return {
        %$ret,
        status  => $status,
        reason  => '',
    };
}

sub run_script {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $base = File::Spec->catdir(
        $DIR,
        $args{xml}->{id}->{value}
    );

    my $full_path = File::Spec->rel2abs(
        $args{execute},
        $base
    );

    my $ret = FileUtils::file_execute(
        %args,
        cmd     => $full_path,
        dir     => $base,
        env     => { %ENV,
                     GA_LOG_DIR => $args{logdir},
        },
        continue_on_error => 1,
    );

    return _process_script_results($ret, %args);
}

sub run_manual {
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    return {
        status => COMPLIANCE_RESULT_MANUAL,
        reason => $args{xml}->{detect}->{value}
    };
}

sub run_perl_script {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $base = File::Spec->catdir($DIR, $args{xml}->{id}->{value});

    my $full_path = File::Spec->rel2abs($args{execute}, $base);

    my $full_lib_path = File::Spec->rel2abs('perl/lib', $ENV{GA_SCRIPT_DIR});

    # Run perl script with the same perl executable this script was runned
    my $full_cmd =
        '"' . $^X . '"' . ' -I"'
      . $full_lib_path
      . '" -Ilib "'
      . $full_path . '"';

    my $ret = FileUtils::file_execute(
        %args,
        cmd               => $full_cmd,
        dir               => $base,
        env               => {%ENV, GA_LOG_DIR => $args{logdir},},
        continue_on_error => 1,
    );

    return _process_script_results($ret, %args);
}

sub _process_script_results {
    my $ret = shift;
    my (%args) = @_;

    my $status = exitcode_to_status($ret->{exitcode}),

    my $rule_log_dir =
    File::Spec->catdir($ENV{GA_LOG_DIR}, $args{xml}->{id}->{value});
    my $filename = 'summary_'.$ENV{GA_SUBJOB_ID}.'.txt';

    my $summary_file = File::Spec->catfile($rule_log_dir, $filename);
    my $reason = '';


    if (-f $summary_file) {
        open my $SUMMARY, '<:utf8', $summary_file
          or fatal("cannot open [$summary_file]: [$!]");

        local $/ = undef;

        $reason = <$SUMMARY>;
    }
    elsif ($status eq exitcode_to_status(COMPLIANCE_RESULT_NON_COMPLIANT)
        || $status eq exitcode_to_status(COMPLIANCE_RESULT_INDETERMINATE))
    {
        # Some reason for INDETERMINATE or NON_COMPLIANT required.
        # If there's no 'summary.txt' file it's probably error

        $reason =
          "Compliance rule execution $status (exit code $ret->{exitcode}).\n";
        $reason .= "Trace:\n";

        FileUtils::file_foreach_line($ret->{stderr},
            sub { $reason .= "\n" . shift; return 1; });
    }

    return {
        %$ret,
        status => $status,
        reason => $reason,
    };
}

my %METHODS = (
    'manual'      => \&run_manual,
    'command'     => \&run_command,
    'script'      => \&run_script,
    'perl-script' => \&run_perl_script
);
Hash::Util::lock_keys(%METHODS);

sub text_element_found {

    my ($elem, $ref) = @_;

    my @nodes = $elem->getChildNodes();

    foreach my $node (@nodes) {

        my $value = $node->getTextContent();
        chomp $value;
        $value =~ s/^\s*//;
        $value =~ s/\s*$//;

        if ($value) {

            $$ref = $value;
            return;
        }
    }
}

sub process_file {

    my $file = shift;
    my $path = File::Spec->catfile($DIR, $file);
    #
    # xml files only
    #
    if (!-f $path) {

        return 1;
    }

    if ($file !~ /\.xml$/i) {

        return 1;
    }

    my $xml = XMLUtils::xml_parse(
        xml => $path,
    );

    my ($status, $reason);

    my $is_excluded = 0;
    if (exists $exclude_checks{ $xml->{id}->{value} }) {
        $is_excluded = 1;
    }


    if (!exists $xml->{detect} || !$xml->{detect}->{value}) {

        $status = COMPLIANCE_RESULT_INDETERMINATE;
        $reason = 'No check.';

    } else {

        my $logdir =  File::Spec->catdir(
            $ENV{GA_LOG_DIR},
            $xml->{id}->{value}
        );

        my (undef, $logfile) =  FileUtils::file_temp_filename(
            'provisioninig_trace_XXXXXX',
            DIR => $logdir,
            SUFFIX => '.log',
            CLEANUP => 0,
        );

        Log::Log4perl->easy_init(
            { level => $ENV{DEBUG_LEVEL},
              file  => $logfile,
              layout=> "[\%d] [\%p]:\t\%m\%n",
            },
        );

        if (!exists $METHODS{lc $xml->{detect}->{method}}) {

            $status = COMPLIANCE_RESULT_INDETERMINATE;
            $reason = 'Detect method "' .$xml->{detect}->{method} . '" is Not implemented.';

        } else {

            my $ret = $METHODS{$xml->{detect}->{method}}->(
                execute => $xml->{detect}->{value},
                logdir  => $logdir,
                xml     => $xml,
            );

            $status = $ret->{status};
            $reason = $ret->{reason};
        }
    }

    my $check = {code       => $xml->{id}->{value},
                 name       => $xml->{name}->{value},
                 result     => $status,
                 message    => $reason,
                 excluded   => $is_excluded,
    };

    if ($status eq COMPLIANCE_RESULT_NON_COMPLIANT) {
        $check->{recommendation} = $xml->{recommendation}->{value};
        $check->{recommendation} =~ s/^\s+|\s+$//g; # trim spaces
    }

    $result->check_result($check);
    #
    # Restore
    #
    Log::Log4perl->easy_init(
        { level => $ENV{DEBUG_LEVEL},
          file  => "STDOUT",
          layout=> "[\%d] [\%p]:\t\%m\%n",
        },
    );

#    FileUtils::log(Dumper($check));

    return 1;
}

FileUtils::dir_foreach_entry(
    $DIR,
    sub {
#        return 1 if ( $_[0] ne "$ENV{GAC_CHECK_ID}\.xml" );
        return process_file(@_); },
    undef,
);
#
# store result
#
$result->save();

exit 0;
