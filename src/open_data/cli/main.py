"""
CLI for Open Data Platform.

Usage:
    opendata db init          # Initialize the database
    opendata db status        # Check database connection and stats
    opendata ingest worldbank # Ingest World Bank data
    opendata query ...        # Query data
    opendata web              # Start web dashboard
"""

from datetime import datetime
from typing import Annotated, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from open_data import __version__
from open_data.config import (
    COUNTRIES,
    COUNTRY_CODES,
    WORLD_BANK_INDICATORS,
    UCDP_INDICATORS,
    Region,
    get_countries_by_region,
    settings,
)

# Initialize Typer app
app = typer.Typer(
    name="opendata",
    help="Open Data Platform - World Bank, IMF, and more",
    add_completion=False,
)

# Sub-commands
db_app = typer.Typer(help="Database management commands")
ingest_app = typer.Typer(help="Data ingestion commands")
query_app = typer.Typer(help="Query and explore data")
export_app = typer.Typer(help="Export data to files")
catalog_app = typer.Typer(help="Browse indicator catalog")
analyze_app = typer.Typer(help="Statistical analysis and forecasting")

app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(query_app, name="query")
app.add_typer(export_app, name="export")
app.add_typer(catalog_app, name="catalog")
app.add_typer(analyze_app, name="analyze")

console = Console()


# =============================================================================
# VERSION CALLBACK
# =============================================================================


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        rprint(f"[bold blue]Open Data Platform[/bold blue] v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        Optional[bool],
        typer.Option("--version", "-v", callback=version_callback, is_eager=True),
    ] = None,
) -> None:
    """Open Data Platform - Economic and social data analysis."""
    pass


# =============================================================================
# DATABASE COMMANDS
# =============================================================================


@db_app.command("init")
def db_init(
    drop: Annotated[bool, typer.Option("--drop", help="Drop existing tables")] = False,
) -> None:
    """Initialize the database schema."""
    from open_data.db.connection import check_connection, init_db

    with console.status("[bold green]Checking database connection..."):
        if not check_connection():
            rprint("[red]Error: Cannot connect to database![/red]")
            rprint(f"Connection string: {settings.database_url}")
            rprint("\n[yellow]Make sure PostgreSQL is running:[/yellow]")
            rprint("  docker-compose up -d postgres")
            raise typer.Exit(1)

    if drop:
        if not typer.confirm("This will DELETE all data. Continue?"):
            raise typer.Abort()

    with console.status("[bold green]Initializing database..."):
        init_db(drop_existing=drop)

    rprint("[green]Database initialized successfully![/green]")


@db_app.command("status")
def db_status() -> None:
    """Check database connection and show statistics."""
    from open_data.db.connection import check_connection, get_table_stats

    # Check connection
    with console.status("[bold green]Checking connection..."):
        connected = check_connection()

    if not connected:
        rprint("[red]Error: Cannot connect to database![/red]")
        rprint(f"\nDatabase URL: {settings.postgres_host}:{settings.postgres_port}")
        rprint("\n[yellow]Start PostgreSQL with:[/yellow]")
        rprint("  docker-compose up -d postgres")
        raise typer.Exit(1)

    rprint("[green]Database connection OK[/green]\n")

    # Get stats
    try:
        stats = get_table_stats()

        table = Table(title="Table Statistics")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", justify="right", style="green")

        for name, count in stats.items():
            table.add_row(name, f"{count:,}")

        console.print(table)

    except Exception as e:
        rprint(f"[yellow]Could not get table stats: {e}[/yellow]")
        rprint("[yellow]Tables may not be created yet. Run: opendata db init[/yellow]")


@db_app.command("url")
def db_url() -> None:
    """Show database connection URL."""
    rprint(f"[cyan]Database URL:[/cyan] {settings.database_url}")


# =============================================================================
# INGESTION COMMANDS
# =============================================================================


@ingest_app.command("worldbank")
def ingest_worldbank(
    indicators: Annotated[
        Optional[str],
        typer.Option("--indicators", "-i", help="Comma-separated indicator codes"),
    ] = None,
    countries: Annotated[
        Optional[str],
        typer.Option("--countries", "-c", help="Comma-separated country codes"),
    ] = None,
    region: Annotated[
        Optional[str],
        typer.Option("--region", "-r", help="Region to filter countries"),
    ] = None,
    start_year: Annotated[
        int,
        typer.Option("--start", "-s", help="Start year"),
    ] = 1960,
    end_year: Annotated[
        Optional[int],
        typer.Option("--end", "-e", help="End year"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be fetched without storing"),
    ] = False,
) -> None:
    """
    Ingest data from World Bank API.

    Examples:
        opendata ingest worldbank
        opendata ingest worldbank -i NY.GDP.MKTP.CD,NY.GDP.PCAP.CD
        opendata ingest worldbank -c ARG,BRA,CHL
        opendata ingest worldbank -r AMERICA
    """
    from open_data.ingestion.world_bank import WorldBankCollector

    # Parse indicators
    indicator_list = None
    if indicators:
        indicator_list = [i.strip() for i in indicators.split(",")]
    else:
        indicator_list = list(WORLD_BANK_INDICATORS.keys())

    # Parse countries
    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",")]
    elif region:
        try:
            r = Region(region.upper())
            country_list = [c.iso3 for c in get_countries_by_region(r)]
        except ValueError:
            rprint(f"[red]Invalid region: {region}[/red]")
            rprint(f"Valid regions: {[r.value for r in Region]}")
            raise typer.Exit(1)

    # Show what will be fetched
    rprint("\n[bold]World Bank Data Ingestion[/bold]\n")
    rprint(f"  Indicators: {len(indicator_list)}")
    rprint(f"  Countries:  {len(country_list or COUNTRY_CODES)}")
    rprint(f"  Years:      {start_year} - {end_year or datetime.now().year}")

    if dry_run:
        rprint("\n[yellow]Dry run - no data will be stored[/yellow]")

        # Show sample indicators
        table = Table(title="Sample Indicators")
        table.add_column("Code", style="cyan")
        table.add_column("Name")

        for code in indicator_list[:10]:
            name = WORLD_BANK_INDICATORS.get(code, code)
            table.add_row(code, name)

        if len(indicator_list) > 10:
            table.add_row("...", f"and {len(indicator_list) - 10} more")

        console.print(table)
        return

    # Confirm
    if not typer.confirm("\nProceed with ingestion?"):
        raise typer.Abort()

    # Run ingestion
    collector = WorldBankCollector(
        countries=country_list,
        start_year=start_year,
        end_year=end_year,
        indicators=indicator_list,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching data from World Bank...", total=None)
        result = collector.run()
        progress.update(task, completed=True)

    # Show results
    if result.status == "completed":
        rprint(f"\n[green]Ingestion completed![/green]")
        rprint(f"  Records processed: {result.records_processed:,}")
        rprint(f"  Duration: {result.duration_seconds:.1f}s")
    else:
        rprint(f"\n[red]Ingestion failed![/red]")
        for error in result.errors[:5]:
            rprint(f"  [red]{error}[/red]")


@ingest_app.command("imf")
def ingest_imf(
    indicators: Annotated[
        Optional[str],
        typer.Option("--indicators", "-i", help="Comma-separated indicator codes"),
    ] = None,
    countries: Annotated[
        Optional[str],
        typer.Option("--countries", "-c", help="Comma-separated country codes"),
    ] = None,
    region: Annotated[
        Optional[str],
        typer.Option("--region", "-r", help="Region to filter countries"),
    ] = None,
    start_year: Annotated[
        int,
        typer.Option("--start", "-s", help="Start year"),
    ] = 1960,
    end_year: Annotated[
        Optional[int],
        typer.Option("--end", "-e", help="End year"),
    ] = None,
) -> None:
    """
    Ingest data from IMF API.

    Examples:
        opendata ingest imf
        opendata ingest imf -i PCPI_PC_CP_A_PT,ENDA_XDC_USD_RATE
        opendata ingest imf -c ARG,BRA,CHL
        opendata ingest imf -r AMERICA
    """
    from open_data.ingestion.imf import IMF_INDICATORS, IMFCollector

    # Parse indicators
    indicator_list = None
    if indicators:
        indicator_list = [i.strip() for i in indicators.split(",")]
    else:
        indicator_list = list(IMF_INDICATORS.keys())

    # Parse countries
    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",")]
    elif region:
        try:
            r = Region(region.upper())
            country_list = [c.iso3 for c in get_countries_by_region(r)]
        except ValueError:
            rprint(f"[red]Invalid region: {region}[/red]")
            raise typer.Exit(1)

    rprint("\n[bold]IMF Data Ingestion[/bold]\n")
    rprint(f"  Indicators: {len(indicator_list)}")
    rprint(f"  Countries:  {len(country_list or COUNTRY_CODES)}")
    rprint(f"  Years:      {start_year} - {end_year or datetime.now().year}")

    if not typer.confirm("\nProceed with ingestion?"):
        raise typer.Abort()

    collector = IMFCollector(
        countries=country_list,
        start_year=start_year,
        end_year=end_year,
        indicators=indicator_list,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching data from IMF...", total=None)
        result = collector.run()
        progress.update(task, completed=True)

    if result.status == "completed":
        rprint(f"\n[green]Ingestion completed![/green]")
        rprint(f"  Records processed: {result.records_processed:,}")
        rprint(f"  Duration: {result.duration_seconds:.1f}s")
    else:
        rprint(f"\n[red]Ingestion failed![/red]")
        for error in result.errors[:5]:
            rprint(f"  [red]{error}[/red]")




@ingest_app.command("ucdp")
def ingest_ucdp(
    indicators: Annotated[
        Optional[str],
        typer.Option("--indicators", "-i", help="Comma-separated indicator codes"),
    ] = None,
    countries: Annotated[
        Optional[str],
        typer.Option("--countries", "-c", help="Comma-separated country codes"),
    ] = None,
    region: Annotated[
        Optional[str],
        typer.Option("--region", "-r", help="Region to filter countries"),
    ] = None,
    start_year: Annotated[
        int,
        typer.Option("--start", "-s", help="Start year"),
    ] = 1989,
    end_year: Annotated[
        Optional[int],
        typer.Option("--end", "-e", help="End year"),
    ] = None,
) -> None:
    """
    Ingest conflict data from Uppsala Conflict Data Program (UCDP).

    Includes battle deaths, non-state conflict deaths, and one-sided violence.

    Examples:
        opendata ingest ucdp
        opendata ingest ucdp -i UCDP.BD.TOTAL,UCDP.NS.TOTAL
        opendata ingest ucdp -c COL,MEX,IRN -s 2000
        opendata ingest ucdp -r MIDDLE_EAST
    """
    from open_data.ingestion.ucdp import UCDPCollector

    # Parse indicators
    indicator_list = None
    if indicators:
        indicator_list = [i.strip() for i in indicators.split(",")]
    else:
        indicator_list = list(UCDP_INDICATORS.keys())

    # Parse countries
    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",")]
    elif region:
        try:
            r = Region(region.upper())
            country_list = [c.iso3 for c in get_countries_by_region(r)]
        except ValueError:
            rprint(f"[red]Invalid region: {region}[/red]")
            raise typer.Exit(1)

    rprint("\n[bold]UCDP Conflict Data Ingestion[/bold]\n")
    rprint(f"  Indicators: {len(indicator_list)}")
    rprint(f"  Countries:  {len(country_list or COUNTRY_CODES)}")
    rprint(f"  Years:      {start_year} - {end_year or datetime.now().year}")
    rprint("\n[dim]Data includes: battle deaths, non-state conflicts, one-sided violence[/dim]")

    if not typer.confirm("\nProceed with ingestion?"):
        raise typer.Abort()

    collector = UCDPCollector(
        countries=country_list,
        start_year=start_year,
        end_year=end_year,
        indicators=indicator_list,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching data from UCDP...", total=None)
        result = collector.collect()
        progress.update(task, completed=True)

    if result.status == "completed":
        rprint(f"\n[green]Ingestion completed![/green]")
        rprint(f"  Records processed: {result.records_processed:,}")
        rprint(f"  Duration: {result.duration_seconds:.1f}s")
    else:
        rprint(f"\n[red]Ingestion failed![/red]")
        for error in result.errors[:5]:
            rprint(f"  [red]{error}[/red]")

@ingest_app.command("all")
def ingest_all(
    start_year: Annotated[
        int,
        typer.Option("--start", "-s", help="Start year"),
    ] = 2000,
    end_year: Annotated[
        Optional[int],
        typer.Option("--end", "-e", help="End year"),
    ] = None,
) -> None:
    """Ingest data from all sources (World Bank + IMF)."""
    from open_data.ingestion.imf import IMFCollector
    from open_data.ingestion.world_bank import WorldBankCollector

    rprint("\n[bold]Full Data Ingestion[/bold]\n")
    rprint(f"  Sources: World Bank, IMF, UCDP")
    rprint(f"  Years:   {start_year} - {end_year or datetime.now().year}")

    if not typer.confirm("\nThis may take several minutes. Proceed?"):
        raise typer.Abort()

    # World Bank
    rprint("\n[cyan]1/2 World Bank[/cyan]")
    wb_collector = WorldBankCollector(start_year=start_year, end_year=end_year)
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        progress.add_task("Fetching World Bank data...", total=None)
        wb_result = wb_collector.run()

    rprint(f"  Records: {wb_result.records_processed:,} ({wb_result.status})")

    # IMF
    rprint("\n[cyan]2/2 IMF[/cyan]")
    imf_collector = IMFCollector(start_year=start_year, end_year=end_year)
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        progress.add_task("Fetching IMF data...", total=None)
        imf_result = imf_collector.run()

    rprint(f"  Records: {imf_result.records_processed:,} ({imf_result.status})")

    total = wb_result.records_processed + imf_result.records_processed
    rprint(f"\n[green]Total records ingested: {total:,}[/green]")


@ingest_app.command("list-indicators")
def list_indicators(
    source: Annotated[
        str,
        typer.Option("--source", "-S", help="Source (wb, imf, all)"),
    ] = "all",
    search: Annotated[
        Optional[str],
        typer.Option("--search", "-s", help="Search term"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Max results"),
    ] = 20,
) -> None:
    """List available indicators from World Bank and IMF."""
    from open_data.core.catalog import search_indicators

    df = search_indicators(query=search, source=source.upper() if source != "all" else None)

    if df.empty:
        rprint("[yellow]No indicators found[/yellow]")
        return

    table = Table(title=f"Indicators (showing {min(limit, len(df))} of {len(df)})")
    table.add_column("Code", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("Source", style="yellow")
    table.add_column("Category")

    for _, row in df.head(limit).iterrows():
        table.add_row(row["code"], row["name"][:50], row["source"], row["category"])

    console.print(table)


# =============================================================================
# QUERY COMMANDS
# =============================================================================


@query_app.command("countries")
def list_countries(
    region: Annotated[
        Optional[str],
        typer.Option("--region", "-r", help="Filter by region"),
    ] = None,
) -> None:
    """List configured countries."""
    table = Table(title="Countries")
    table.add_column("ISO3", style="cyan")
    table.add_column("ISO2")
    table.add_column("Name")
    table.add_column("Region", style="yellow")
    table.add_column("Subregion")

    countries = COUNTRIES.values()
    if region:
        try:
            r = Region(region.upper())
            countries = get_countries_by_region(r)
        except ValueError:
            rprint(f"[red]Invalid region: {region}[/red]")
            rprint(f"Valid regions: {[r.value for r in Region]}")
            raise typer.Exit(1)

    for c in sorted(countries, key=lambda x: (x.region.value, x.name)):
        table.add_row(c.iso3, c.iso2, c.name, c.region.value, c.subregion)

    console.print(table)
    rprint(f"\nTotal: {len(list(countries))} countries")


@query_app.command("regions")
def list_regions() -> None:
    """List available regions."""
    table = Table(title="Regions")
    table.add_column("Region", style="cyan")
    table.add_column("Countries", justify="right")

    for r in Region:
        count = len(get_countries_by_region(r))
        table.add_row(r.value, str(count))

    console.print(table)


@query_app.command("data")
def query_data(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    countries: Annotated[
        Optional[str],
        typer.Option("--countries", "-c", help="Comma-separated country codes"),
    ] = None,
    start_year: Annotated[
        int,
        typer.Option("--start", "-s", help="Start year"),
    ] = 2010,
    end_year: Annotated[
        Optional[int],
        typer.Option("--end", "-e", help="End year"),
    ] = None,
    output: Annotated[
        Optional[str],
        typer.Option("--output", "-o", help="Output file (csv)"),
    ] = None,
) -> None:
    """
    Query data for a specific indicator.

    Example:
        opendata query data NY.GDP.PCAP.CD -c ARG,BRA,CHL -s 2015
    """
    from open_data.db.connection import session_scope
    from open_data.db.models import Country, Indicator, Observation

    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",")]

    with session_scope() as session:
        query = (
            session.query(
                Country.iso3_code,
                Country.name,
                Observation.year,
                Observation.value,
            )
            .join(Observation, Country.id == Observation.country_id)
            .join(Indicator, Indicator.id == Observation.indicator_id)
            .filter(Indicator.code == indicator)
            .filter(Observation.year >= start_year)
        )

        if end_year:
            query = query.filter(Observation.year <= end_year)

        if country_list:
            query = query.filter(Country.iso3_code.in_(country_list))

        query = query.order_by(Country.iso3_code, Observation.year)
        results = query.all()

    if not results:
        rprint("[yellow]No data found[/yellow]")
        return

    # Create table
    table = Table(title=f"Data: {indicator}")
    table.add_column("Country", style="cyan")
    table.add_column("Name")
    table.add_column("Year", justify="right")
    table.add_column("Value", justify="right", style="green")

    for iso3, name, year, value in results[:50]:
        val_str = f"{value:,.2f}" if value else "N/A"
        table.add_row(iso3, name, str(year), val_str)

    if len(results) > 50:
        table.add_row("...", "", "", f"({len(results)} total rows)")

    console.print(table)

    # Export if requested
    if output:
        import pandas as pd

        df = pd.DataFrame(results, columns=["iso3", "name", "year", "value"])
        df.to_csv(output, index=False)
        rprint(f"\n[green]Exported to {output}[/green]")


# =============================================================================
# WEB COMMAND
# =============================================================================


@app.command("web")
def start_web(
    port: Annotated[int, typer.Option("--port", "-p", help="Port number")] = 8501,
    host: Annotated[str, typer.Option("--host", "-h", help="Host address")] = "localhost",
) -> None:
    """Start the Streamlit web dashboard."""
    import subprocess
    import sys

    web_app = "web/app.py"

    rprint(f"\n[bold blue]Starting Open Data Dashboard[/bold blue]")
    rprint(f"  URL: http://{host}:{port}\n")

    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                web_app,
                "--server.port",
                str(port),
                "--server.address",
                host,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        rprint(f"[red]Error starting web server: {e}[/red]")
        raise typer.Exit(1)
    except FileNotFoundError:
        rprint("[red]Streamlit not found. Install with: pip install streamlit[/red]")
        raise typer.Exit(1)


# =============================================================================
# INFO COMMAND
# =============================================================================


@app.command("info")
def show_info() -> None:
    """Show configuration information."""
    panel = Panel.fit(
        f"""[bold]Open Data Platform[/bold] v{__version__}

[cyan]Database:[/cyan]
  Host: {settings.postgres_host}:{settings.postgres_port}
  Database: {settings.postgres_db}

[cyan]Data Sources:[/cyan]
  World Bank API: {settings.world_bank_api_base}
  IMF API: {settings.imf_api_base}

[cyan]Configuration:[/cyan]
  Countries: {len(COUNTRY_CODES)}
  Default indicators: {len(WORLD_BANK_INDICATORS)}
  Year range: {settings.default_start_year} - {settings.default_end_year}
""",
        title="Configuration",
        border_style="blue",
    )
    console.print(panel)


# =============================================================================
# EXPORT COMMANDS
# =============================================================================


@export_app.command("csv")
def export_csv(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    countries: Annotated[
        Optional[str],
        typer.Option("--countries", "-c", help="Comma-separated country codes"),
    ] = None,
    start_year: Annotated[int, typer.Option("--start", "-s")] = 2000,
    end_year: Annotated[Optional[int], typer.Option("--end", "-e")] = None,
    output: Annotated[Optional[str], typer.Option("--output", "-o")] = None,
) -> None:
    """Export indicator data to CSV."""
    from open_data.core.export import DataExporter, ExportConfig, ExportFormat

    country_list = [c.strip().upper() for c in countries.split(",")] if countries else None

    exporter = DataExporter(ExportConfig(format=ExportFormat.CSV))
    filepath = exporter.export_indicator(
        indicator,
        countries=country_list,
        start_year=start_year,
        end_year=end_year,
        filename=output,
    )

    rprint(f"[green]Exported to: {filepath}[/green]")


@export_app.command("excel")
def export_excel(
    country: Annotated[str, typer.Argument(help="Country code (ISO3)")],
    output: Annotated[Optional[str], typer.Option("--output", "-o")] = None,
) -> None:
    """Export country report to Excel."""
    from open_data.core.export import create_country_report

    with console.status(f"Creating report for {country}..."):
        filepath = create_country_report(country.upper())

    rprint(f"[green]Report saved to: {filepath}[/green]")


@export_app.command("timeseries")
def export_timeseries(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    countries: Annotated[str, typer.Option("--countries", "-c", help="Comma-separated country codes")],
    start_year: Annotated[int, typer.Option("--start", "-s")] = 2000,
    end_year: Annotated[Optional[int], typer.Option("--end", "-e")] = None,
) -> None:
    """Export time series with countries as columns."""
    from open_data.core.export import DataExporter, ExportConfig, ExportFormat

    country_list = [c.strip().upper() for c in countries.split(",")]

    exporter = DataExporter(ExportConfig(format=ExportFormat.CSV))
    filepath = exporter.export_time_series(
        indicator,
        countries=country_list,
        start_year=start_year,
        end_year=end_year,
        pivot=True,
    )

    rprint(f"[green]Exported to: {filepath}[/green]")


# =============================================================================
# CATALOG COMMANDS
# =============================================================================


@catalog_app.command("search")
def catalog_search(
    query: Annotated[str, typer.Argument(help="Search term")],
    category: Annotated[Optional[str], typer.Option("--category", "-c")] = None,
    source: Annotated[Optional[str], typer.Option("--source", "-s")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """Search the indicator catalog."""
    from open_data.core.catalog import search_indicators

    df = search_indicators(query=query, category=category, source=source)

    if df.empty:
        rprint("[yellow]No indicators found[/yellow]")
        return

    table = Table(title=f"Search Results: '{query}'")
    table.add_column("Code", style="cyan")
    table.add_column("Name")
    table.add_column("Source", style="yellow")
    table.add_column("Category", style="green")

    for _, row in df.head(limit).iterrows():
        table.add_row(row["code"], row["name"][:40], row["source"], row["category"])

    console.print(table)
    rprint(f"\nFound {len(df)} indicators")


@catalog_app.command("categories")
def catalog_categories() -> None:
    """List indicator categories."""
    from open_data.core.catalog import list_categories

    categories = list_categories()

    table = Table(title="Indicator Categories")
    table.add_column("Category", style="cyan")

    for cat in categories:
        table.add_row(cat.title())

    console.print(table)


@catalog_app.command("show")
def catalog_show(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
) -> None:
    """Show details for an indicator."""
    from open_data.core.catalog import get_indicator_info

    info = get_indicator_info(indicator)

    if not info:
        rprint(f"[red]Indicator not found: {indicator}[/red]")
        raise typer.Exit(1)

    panel = Panel.fit(
        f"""[bold]{info.name}[/bold]

[cyan]Code:[/cyan]        {info.code}
[cyan]Source:[/cyan]      {info.source}
[cyan]Category:[/cyan]    {info.category}
[cyan]Frequency:[/cyan]   {info.frequency}

[cyan]Description:[/cyan]
{info.description or 'No description available'}
""",
        title="Indicator Details",
        border_style="blue",
    )
    console.print(panel)


# =============================================================================
# COMPARE COMMAND
# =============================================================================


@query_app.command("compare")
def query_compare(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    countries: Annotated[str, typer.Option("--countries", "-c", help="Comma-separated country codes")],
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
) -> None:
    """Compare countries for an indicator."""
    from open_data.core.query import DataQuery

    country_list = [c.strip().upper() for c in countries.split(",")]
    target_year = year or datetime.now().year - 1

    df = DataQuery.compare_countries(indicator, country_list, target_year)

    if df.empty:
        rprint("[yellow]No data found[/yellow]")
        return

    table = Table(title=f"Comparison: {indicator} ({target_year})")
    table.add_column("Rank", justify="right", style="dim")
    table.add_column("Country", style="cyan")
    table.add_column("Value", justify="right", style="green")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        val_str = f"{row['value']:,.2f}" if row['value'] else "N/A"
        table.add_row(str(i), row['country_name'], val_str)

    console.print(table)


@query_app.command("summary")
def query_summary() -> None:
    """Show summary of available data."""
    from open_data.core.query import get_available_data_summary

    with console.status("Analyzing data..."):
        df = get_available_data_summary()

    if df.empty:
        rprint("[yellow]No data in database. Run: opendata ingest worldbank[/yellow]")
        return

    table = Table(title="Data Summary")
    table.add_column("Source", style="yellow")
    table.add_column("Category")
    table.add_column("Indicator", style="cyan")
    table.add_column("Obs", justify="right")
    table.add_column("Countries", justify="right")
    table.add_column("Years")

    for _, row in df.head(30).iterrows():
        table.add_row(
            row["source"] or "",
            row["category"] or "",
            row["code"][:20],
            f"{row['observations']:,}",
            str(row["countries"]),
            f"{row['min_year']}-{row['max_year']}",
        )

    console.print(table)
    rprint(f"\nTotal indicators: {len(df)}")


@query_app.command("latest")
def query_latest(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    region: Annotated[Optional[str], typer.Option("--region", "-r")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """Get latest values for an indicator."""
    from open_data.core.query import DataQuery

    country_list = None
    if region:
        try:
            r = Region(region.upper())
            country_list = [c.iso3 for c in get_countries_by_region(r)]
        except ValueError:
            rprint(f"[red]Invalid region: {region}[/red]")
            raise typer.Exit(1)

    df = DataQuery.get_latest_values(indicator, countries=country_list)

    if df.empty:
        rprint("[yellow]No data found[/yellow]")
        return

    table = Table(title=f"Latest Values: {indicator}")
    table.add_column("Country", style="cyan")
    table.add_column("Year", justify="right")
    table.add_column("Value", justify="right", style="green")

    for _, row in df.head(limit).iterrows():
        val_str = f"{row['value']:,.2f}" if row['value'] else "N/A"
        table.add_row(row['country_name'], str(row['year']), val_str)

    console.print(table)


# =============================================================================
# ANALYSIS COMMANDS
# =============================================================================


@analyze_app.command("trend")
def analyze_trend(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    country: Annotated[str, typer.Argument(help="Country code (ISO3)")],
    start_year: Annotated[int, typer.Option("--start", "-s")] = 1990,
    end_year: Annotated[Optional[int], typer.Option("--end", "-e")] = None,
) -> None:
    """
    Analyze trend for an indicator and country.

    Example:
        opendata analyze trend NY.GDP.PCAP.CD ARG
    """
    from open_data.core.timeseries import analyze_indicator_trend

    with console.status("Analyzing trend..."):
        try:
            trend = analyze_indicator_trend(
                indicator, country.upper(), start_year, end_year
            )
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    # Display results
    trend_colors = {
        "increasing": "green",
        "decreasing": "red",
        "stable": "yellow",
        "volatile": "orange1",
    }
    color = trend_colors.get(trend.trend_type.value, "white")

    panel = Panel.fit(
        f"""[bold]Trend Analysis: {indicator}[/bold]
Country: {country.upper()}

[{color}]Trend Type: {trend.trend_type.value.upper()}[/{color}]

[cyan]Statistics:[/cyan]
  Start Value:      {trend.start_value:,.2f}
  End Value:        {trend.end_value:,.2f}
  Total Change:     {trend.total_change_pct:+.1f}%
  Avg Growth Rate:  {trend.avg_growth_rate:+.2f}% per year
  Volatility:       {trend.volatility:.2f}%

[cyan]Regression:[/cyan]
  Slope:            {trend.slope:.4f}
  R-squared:        {trend.r_squared:.3f}
  P-value:          {trend.p_value:.4f} {'(significant)' if trend.p_value < 0.05 else '(not significant)'}
""",
        title="Trend Analysis",
        border_style=color,
    )
    console.print(panel)


@analyze_app.command("forecast")
def analyze_forecast(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    country: Annotated[str, typer.Argument(help="Country code (ISO3)")],
    periods: Annotated[int, typer.Option("--periods", "-p")] = 5,
    method: Annotated[str, typer.Option("--method", "-m")] = "holt",
    start_year: Annotated[int, typer.Option("--start", "-s")] = 1990,
) -> None:
    """
    Forecast an indicator for a country.

    Methods: linear, exponential, holt, moving_average

    Example:
        opendata analyze forecast NY.GDP.PCAP.CD ARG -p 5
    """
    from open_data.core.timeseries import forecast_indicator

    with console.status("Generating forecast..."):
        try:
            forecast = forecast_indicator(
                indicator, country.upper(), periods, method, start_year
            )
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    # Display results
    table = Table(title=f"Forecast: {indicator} ({country.upper()})")
    table.add_column("Year", style="cyan", justify="right")
    table.add_column("Forecast", justify="right", style="green")
    table.add_column("Lower 95%", justify="right", style="dim")
    table.add_column("Upper 95%", justify="right", style="dim")

    for i, year in enumerate(forecast.forecast_years):
        table.add_row(
            str(year),
            f"{forecast.forecast_values[i]:,.2f}",
            f"{forecast.confidence_lower[i]:,.2f}",
            f"{forecast.confidence_upper[i]:,.2f}",
        )

    console.print(table)

    rprint(f"\n[cyan]Method:[/cyan] {forecast.method.value}")
    if forecast.mape:
        rprint(f"[cyan]MAPE:[/cyan] {forecast.mape:.2f}%")
    if forecast.rmse:
        rprint(f"[cyan]RMSE:[/cyan] {forecast.rmse:.2f}")


@analyze_app.command("correlate")
def analyze_correlation(
    indicator1: Annotated[str, typer.Argument(help="First indicator code")],
    indicator2: Annotated[str, typer.Argument(help="Second indicator code")],
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
    method: Annotated[str, typer.Option("--method", "-m")] = "pearson",
) -> None:
    """
    Calculate correlation between two indicators.

    Example:
        opendata analyze correlate NY.GDP.PCAP.CD FP.CPI.TOTL.ZG
    """
    from open_data.core.statistics import correlate_indicators

    with console.status("Calculating correlation..."):
        try:
            result = correlate_indicators(indicator1, indicator2, year, method)
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    # Color based on correlation strength
    r = result.correlation
    if abs(r) > 0.7:
        color = "green" if r > 0 else "red"
    elif abs(r) > 0.4:
        color = "yellow"
    else:
        color = "dim"

    panel = Panel.fit(
        f"""[bold]Correlation Analysis[/bold]

Indicator 1: {indicator1}
Indicator 2: {indicator2}
Year: {year or 'latest'}

[{color}]Correlation: {result.correlation:+.4f}[/{color}]
Strength: {result.strength}
P-value: {result.p_value:.4f} {'*' if result.is_significant else ''}
Observations: {result.n_observations}
Method: {result.method}

{'[green]Statistically significant (p < 0.05)[/green]' if result.is_significant else '[yellow]Not statistically significant[/yellow]'}
""",
        title="Correlation",
        border_style=color,
    )
    console.print(panel)


@analyze_app.command("cluster")
def analyze_cluster(
    n_clusters: Annotated[int, typer.Option("--clusters", "-n")] = 4,
    preset: Annotated[Optional[str], typer.Option("--preset", "-p")] = None,
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
) -> None:
    """
    Cluster countries by economic indicators.

    Presets: development, economy

    Example:
        opendata analyze cluster -n 4 -p economy
    """
    from open_data.core.clustering import (
        cluster_countries,
        segment_by_development,
        segment_by_economy,
    )

    with console.status("Clustering countries..."):
        try:
            if preset == "development":
                result = segment_by_development(year, n_clusters)
            elif preset == "economy":
                result = segment_by_economy(year, n_clusters)
            else:
                # Default indicators
                indicators = [
                    "NY.GDP.PCAP.CD",
                    "NY.GDP.MKTP.KD.ZG",
                    "FP.CPI.TOTL.ZG",
                    "SL.UEM.TOTL.ZS",
                ]
                result = cluster_countries(indicators, n_clusters, year)
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    # Display results
    rprint(f"\n[bold]Clustering Results[/bold]")
    rprint(f"Method: {result.method}")
    rprint(f"Clusters: {result.n_clusters}")
    if result.silhouette_score:
        rprint(f"Silhouette Score: {result.silhouette_score:.3f}")

    for cluster_id in range(result.n_clusters):
        members = result.get_cluster_members(cluster_id)
        rprint(f"\n[cyan]Cluster {cluster_id + 1}[/cyan] ({len(members)} countries):")
        rprint(f"  {', '.join(members)}")

    # Show cluster centers if available
    if result.cluster_centers is not None:
        rprint("\n[bold]Cluster Centers:[/bold]")
        console.print(result.cluster_centers.round(2).to_string())


@analyze_app.command("rank")
def analyze_rank(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
    ascending: Annotated[bool, typer.Option("--ascending", "-a")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n")] = 20,
) -> None:
    """
    Rank countries by indicator value.

    Example:
        opendata analyze rank NY.GDP.PCAP.CD
    """
    from open_data.core.statistics import rank_countries

    with console.status("Ranking countries..."):
        try:
            df = rank_countries(indicator, year, ascending)
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    if df.empty:
        rprint("[yellow]No data found[/yellow]")
        return

    table = Table(title=f"Country Rankings: {indicator}")
    table.add_column("Rank", justify="right", style="dim")
    table.add_column("Country", style="cyan")
    table.add_column("Value", justify="right", style="green")
    table.add_column("Percentile", justify="right")
    table.add_column("Z-Score", justify="right")

    for _, row in df.head(limit).iterrows():
        table.add_row(
            str(int(row["rank"])),
            row["country_name"],
            f"{row['value']:,.2f}" if row["value"] else "N/A",
            f"{row['percentile']:.1f}%" if row["percentile"] else "N/A",
            f"{row['z_score']:+.2f}" if row["z_score"] else "N/A",
        )

    console.print(table)


@analyze_app.command("similar")
def analyze_similar(
    country: Annotated[str, typer.Argument(help="Reference country code (ISO3)")],
    n: Annotated[int, typer.Option("--count", "-n")] = 5,
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
) -> None:
    """
    Find countries similar to a reference country.

    Example:
        opendata analyze similar ARG -n 5
    """
    from open_data.core.clustering import find_similar_countries

    indicators = [
        "NY.GDP.PCAP.CD",
        "NY.GDP.MKTP.KD.ZG",
        "FP.CPI.TOTL.ZG",
        "SL.UEM.TOTL.ZS",
        "NE.TRD.GNFS.ZS",
    ]

    with console.status(f"Finding countries similar to {country.upper()}..."):
        try:
            df = find_similar_countries(country.upper(), indicators, year, n)
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    table = Table(title=f"Countries Similar to {country.upper()}")
    table.add_column("Rank", justify="right", style="dim")
    table.add_column("Country", style="cyan")
    table.add_column("Similarity", justify="right", style="green")
    table.add_column("Distance", justify="right", style="dim")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        table.add_row(
            str(i),
            row["country"],
            f"{row['similarity']:.3f}",
            f"{row['distance']:.3f}",
        )

    console.print(table)


@analyze_app.command("stats")
def analyze_stats(
    indicator: Annotated[str, typer.Argument(help="Indicator code")],
    year: Annotated[Optional[int], typer.Option("--year", "-y")] = None,
) -> None:
    """
    Show descriptive statistics for an indicator.

    Example:
        opendata analyze stats NY.GDP.PCAP.CD
    """
    from open_data.core.statistics import indicator_statistics

    with console.status("Calculating statistics..."):
        try:
            stats = indicator_statistics(indicator, year)
        except Exception as e:
            rprint(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    panel = Panel.fit(
        f"""[bold]Descriptive Statistics: {indicator}[/bold]
Year: {year or 'latest available'}

[cyan]Central Tendency:[/cyan]
  Mean:     {stats.mean:,.2f}
  Median:   {stats.median:,.2f}

[cyan]Dispersion:[/cyan]
  Std Dev:  {stats.std:,.2f}
  CV:       {stats.cv:.1f}%
  Range:    {stats.min:,.2f} - {stats.max:,.2f}

[cyan]Quartiles:[/cyan]
  Q1 (25%): {stats.q25:,.2f}
  Q2 (50%): {stats.median:,.2f}
  Q3 (75%): {stats.q75:,.2f}

[cyan]Distribution:[/cyan]
  Skewness: {stats.skewness:+.3f}
  Kurtosis: {stats.kurtosis:+.3f}
  N:        {stats.count}
""",
        title="Statistics",
        border_style="cyan",
    )
    console.print(panel)


if __name__ == "__main__":
    app()
