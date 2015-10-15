package ComplianceUtils;

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/lib";
use FileUtils;

use GridApp::Archive;
use GA::Standards::ResultsCodes ':codes';

use base 'Exporter';

my %EXITCODE_NAME = (
    0  => COMPLIANCE_RESULT_INDETERMINATE,
    10 => COMPLIANCE_RESULT_COMPLIANT,
    11 => COMPLIANCE_RESULT_MANUAL,
    12 => COMPLIANCE_RESULT_COMPLIANT_WITH_EXCEPTIONS,
    13 => COMPLIANCE_RESULT_NON_COMPLIANT,
);

my %STATUS_EXITCODE = reverse %EXITCODE_NAME;

our @EXPORT = qw/
  pass_compliance
  fail_compliance
  manual_check_required
  skip_because_of_error
  /;

our @EXPORT_OK = qw/
  parameter_to_list
  exitcode_to_status
  status_to_exitcode
  exit_with_status
  list_to_quoted_list
  unquote_list
  /;

sub exitcode_to_status {
    my $code = shift;

    return $EXITCODE_NAME{$code} if exists $EXITCODE_NAME{$code};

    # If exit code is not known iterpret it as INDETERMINATE
    return COMPLIANCE_RESULT_INDETERMINATE;
}

sub status_to_exitcode {
    my $code = shift;

    return $STATUS_EXITCODE{$code} if exists $STATUS_EXITCODE{$code};

    # If exit code is not known iterpret it as INDETERMINATE
    return $STATUS_EXITCODE{COMPLIANCE_RESULT_INDETERMINATE()};
}

=head1 rule_excluded_from_standard

Mark rule as (temporarry) excluded from a standard because of certain reason
This call will terminate execution of process.
'Message' parameter is optional.

    rule_excluded_from_standard(
        "Rule excluded from compliance standard because of ...");

=cut

sub rule_excluded_from_standard {
    exit_with_status(COMPLIANCE_RESULT_INDETERMINATE, @_);
}

=head1 fail_compliance

Mark compliance rule as failed.
This call will terminate execution of process.
'Message' parameter is optional.

    fail_compliance("Rule failed because of ...");

=cut

sub fail_compliance {
    exit_with_status(COMPLIANCE_RESULT_NON_COMPLIANT, @_);
}

=head1 manual_check_required

Require manual check of database/OS state.
This call will terminate execution of process.
'Message' parameter is optional.

    manual_check_required("Please confirm the following: ...");

=cut

sub manual_check_required {
    exit_with_status(COMPLIANCE_RESULT_MANUAL, @_);
}

=head1 pass_compliance

Mark compliance rule as passed.
This call will terminate execution of process.
'Message' parameter is optional.

    pass_compliance("Please confirm the following: ...");

=cut

sub pass_compliance {
    exit_with_status(COMPLIANCE_RESULT_COMPLIANT, @_);
}

=head1 skip_because_of_error

Skip compliance rule because of unrecoverable error.
This call will terminate execution of process.
'Message' parameter is optional.

    skip_because_of_error("Please confirm the following: ...");

=cut

sub skip_because_of_error {
    exit_with_status(COMPLIANCE_RESULT_INDETERMINATE, @_);
}

sub exit_with_status {
    my ($status, $message) = @_;

    my $exitcode = status_to_exitcode($status);

    exit $exitcode unless $message;

    _write_cmdb_message($message);

    print $message, "\n";

    exit $exitcode;
}

sub _write_cmdb_message {
    my $cmdb_message = _quote_cmdb_message($_[0]);

    FileUtils::file_rewrite("$ENV{GA_LOG_DIR}/summary_$ENV{GA_SUBJOB_ID}.txt", $cmdb_message);
}

sub _quote_cmdb_message {
    my $cmdb_message = shift;

    # Subroutine from the old days, when dmanager had troubles with accepting special chars
    ## Something in CMD or dmanager doesn't like pipes
    #$cmdb_message =~ s/\|/!/g;

    return $cmdb_message;
}

sub parameter_to_list {
    my $param = $_[0];

    # Add prefix
    $param = 'GAC_' . $param unless $param =~ /^GAC_/;

    return unless defined $ENV{$param};
    return unless length $ENV{$param};

    my @list = grep { length($_) } split /\s*,\s*/s, $ENV{$param};
    s/^\s+|\s+$//g for @list;

    return @list;
}

sub list_to_quoted_list {
    return
        map {
            if ( /^[\'\"].+[\'\"]$/ ) {
                $_;
            } else {
                qq/'$_'/;
            }
        } @_;
}

sub unquote_list {
    return
        map {
            s/^[\'\"]|[\'\"]$//g;
            $_;
        } @_;
}

1;

