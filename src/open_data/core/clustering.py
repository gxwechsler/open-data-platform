"""
Country Clustering and Segmentation Module.

Provides clustering analysis for grouping countries:
- K-Means clustering
- Hierarchical clustering
- Principal Component Analysis (PCA)
- Cluster profiling and interpretation
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import dendrogram, fcluster, linkage
from scipy.spatial.distance import pdist
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from open_data.core.query import DataQuery, QueryBuilder


@dataclass
class ClusterResult:
    """Result of clustering analysis."""
    n_clusters: int
    method: str
    labels: dict[str, int]  # country -> cluster
    cluster_sizes: dict[int, int]
    cluster_centers: pd.DataFrame | None
    silhouette_score: float | None
    inertia: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_clusters": self.n_clusters,
            "method": self.method,
            "labels": self.labels,
            "cluster_sizes": self.cluster_sizes,
            "silhouette_score": self.silhouette_score,
            "inertia": self.inertia,
        }

    def get_cluster_members(self, cluster_id: int) -> list[str]:
        """Get countries in a specific cluster."""
        return [c for c, label in self.labels.items() if label == cluster_id]


@dataclass
class PCAResult:
    """Result of PCA analysis."""
    n_components: int
    explained_variance_ratio: list[float]
    cumulative_variance: list[float]
    components: pd.DataFrame  # PC loadings
    transformed_data: pd.DataFrame  # Countries in PC space

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_components": self.n_components,
            "explained_variance_ratio": self.explained_variance_ratio,
            "cumulative_variance": self.cumulative_variance,
        }


@dataclass
class ClusterProfile:
    """Profile of a single cluster."""
    cluster_id: int
    size: int
    countries: list[str]
    mean_values: dict[str, float]
    std_values: dict[str, float]
    characteristic_features: list[str]  # Features where this cluster stands out

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "size": self.size,
            "countries": self.countries,
            "mean_values": self.mean_values,
            "characteristic_features": self.characteristic_features,
        }


class CountryClusterer:
    """
    Cluster countries based on multiple indicators.
    """

    def __init__(self):
        self.data: pd.DataFrame | None = None
        self.scaled_data: np.ndarray | None = None
        self.scaler: StandardScaler | None = None
        self.feature_names: list[str] = []
        self.country_names: list[str] = []

    def prepare_data(
        self,
        indicators: list[str],
        year: int | None = None,
        min_data_points: int = 3,
    ) -> "CountryClusterer":
        """
        Prepare data for clustering.

        Args:
            indicators: List of indicator codes.
            year: Year to use (latest if None).
            min_data_points: Minimum indicators required per country.

        Returns:
            Self for chaining.
        """
        target_year = year or 2022

        # Fetch data for all indicators
        all_data = []
        for indicator in indicators:
            df = (
                QueryBuilder()
                .select(indicator)
                .year(target_year)
                .execute()
                .data
            )
            if not df.empty:
                df = df[["country", "value"]].rename(columns={"value": indicator})
                all_data.append(df)

        if not all_data:
            raise ValueError("No data found for any indicator")

        # Merge all indicators
        merged = all_data[0]
        for df in all_data[1:]:
            merged = merged.merge(df, on="country", how="outer")

        # Filter countries with enough data
        merged = merged.dropna(thresh=min_data_points + 1)  # +1 for country column

        if len(merged) < 3:
            raise ValueError("Not enough countries with sufficient data")

        # Fill remaining NaN with column means
        for col in indicators:
            if col in merged.columns:
                merged[col] = merged[col].fillna(merged[col].mean())

        self.country_names = merged["country"].tolist()
        self.feature_names = [c for c in indicators if c in merged.columns]
        self.data = merged.set_index("country")[self.feature_names]

        # Standardize
        self.scaler = StandardScaler()
        self.scaled_data = self.scaler.fit_transform(self.data)

        return self

    def kmeans(
        self,
        n_clusters: int = 4,
        random_state: int = 42,
    ) -> ClusterResult:
        """
        Perform K-Means clustering.

        Args:
            n_clusters: Number of clusters.
            random_state: Random seed.

        Returns:
            ClusterResult object.
        """
        if self.scaled_data is None:
            raise ValueError("No data prepared. Call prepare_data first.")

        kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
        labels = kmeans.fit_predict(self.scaled_data)

        # Calculate silhouette score
        from sklearn.metrics import silhouette_score
        sil_score = silhouette_score(self.scaled_data, labels) if n_clusters > 1 else None

        # Create label mapping
        label_map = dict(zip(self.country_names, labels.tolist()))

        # Cluster sizes
        cluster_sizes = {}
        for i in range(n_clusters):
            cluster_sizes[i] = int(np.sum(labels == i))

        # Cluster centers (inverse transform to original scale)
        centers_scaled = kmeans.cluster_centers_
        centers = self.scaler.inverse_transform(centers_scaled)
        centers_df = pd.DataFrame(centers, columns=self.feature_names)
        centers_df.index.name = "cluster"

        return ClusterResult(
            n_clusters=n_clusters,
            method="kmeans",
            labels=label_map,
            cluster_sizes=cluster_sizes,
            cluster_centers=centers_df,
            silhouette_score=sil_score,
            inertia=float(kmeans.inertia_),
        )

    def hierarchical(
        self,
        n_clusters: int = 4,
        method: str = "ward",
        metric: str = "euclidean",
    ) -> ClusterResult:
        """
        Perform hierarchical clustering.

        Args:
            n_clusters: Number of clusters.
            method: Linkage method ('ward', 'complete', 'average', 'single').
            metric: Distance metric.

        Returns:
            ClusterResult object.
        """
        if self.scaled_data is None:
            raise ValueError("No data prepared. Call prepare_data first.")

        # Compute linkage
        Z = linkage(self.scaled_data, method=method, metric=metric)

        # Cut tree to get clusters
        labels = fcluster(Z, n_clusters, criterion="maxclust") - 1  # 0-indexed

        # Calculate silhouette score
        from sklearn.metrics import silhouette_score
        sil_score = silhouette_score(self.scaled_data, labels) if n_clusters > 1 else None

        # Create label mapping
        label_map = dict(zip(self.country_names, labels.tolist()))

        # Cluster sizes
        cluster_sizes = {}
        for i in range(n_clusters):
            cluster_sizes[i] = int(np.sum(labels == i))

        # Calculate cluster centers as mean of members
        centers = []
        for i in range(n_clusters):
            mask = labels == i
            center = self.scaler.inverse_transform(
                self.scaled_data[mask].mean(axis=0).reshape(1, -1)
            )[0]
            centers.append(center)

        centers_df = pd.DataFrame(centers, columns=self.feature_names)
        centers_df.index.name = "cluster"

        return ClusterResult(
            n_clusters=n_clusters,
            method=f"hierarchical_{method}",
            labels=label_map,
            cluster_sizes=cluster_sizes,
            cluster_centers=centers_df,
            silhouette_score=sil_score,
            inertia=None,
        )

    def find_optimal_clusters(
        self,
        max_clusters: int = 10,
        method: str = "kmeans",
    ) -> pd.DataFrame:
        """
        Find optimal number of clusters using elbow method and silhouette.

        Args:
            max_clusters: Maximum clusters to test.
            method: Clustering method.

        Returns:
            DataFrame with metrics for each k.
        """
        if self.scaled_data is None:
            raise ValueError("No data prepared. Call prepare_data first.")

        from sklearn.metrics import silhouette_score

        results = []
        for k in range(2, min(max_clusters + 1, len(self.country_names))):
            if method == "kmeans":
                result = self.kmeans(n_clusters=k)
            else:
                result = self.hierarchical(n_clusters=k)

            results.append({
                "n_clusters": k,
                "silhouette": result.silhouette_score,
                "inertia": result.inertia,
            })

        return pd.DataFrame(results)

    def pca(self, n_components: int | None = None) -> PCAResult:
        """
        Perform Principal Component Analysis.

        Args:
            n_components: Number of components (None for all).

        Returns:
            PCAResult object.
        """
        if self.scaled_data is None:
            raise ValueError("No data prepared. Call prepare_data first.")

        if n_components is None:
            n_components = min(self.scaled_data.shape)

        pca = PCA(n_components=n_components)
        transformed = pca.fit_transform(self.scaled_data)

        # Component loadings
        loadings = pd.DataFrame(
            pca.components_.T,
            index=self.feature_names,
            columns=[f"PC{i+1}" for i in range(n_components)],
        )

        # Transformed data
        transformed_df = pd.DataFrame(
            transformed,
            index=self.country_names,
            columns=[f"PC{i+1}" for i in range(n_components)],
        )

        return PCAResult(
            n_components=n_components,
            explained_variance_ratio=pca.explained_variance_ratio_.tolist(),
            cumulative_variance=np.cumsum(pca.explained_variance_ratio_).tolist(),
            components=loadings,
            transformed_data=transformed_df,
        )

    def profile_clusters(self, cluster_result: ClusterResult) -> list[ClusterProfile]:
        """
        Create profiles for each cluster.

        Args:
            cluster_result: Result from clustering.

        Returns:
            List of ClusterProfile objects.
        """
        if self.data is None:
            raise ValueError("No data available")

        profiles = []
        global_means = self.data.mean()
        global_stds = self.data.std()

        for cluster_id in range(cluster_result.n_clusters):
            members = cluster_result.get_cluster_members(cluster_id)
            cluster_data = self.data.loc[members]

            means = cluster_data.mean()
            stds = cluster_data.std()

            # Find characteristic features (> 1 std from global mean)
            z_scores = (means - global_means) / global_stds
            characteristic = z_scores[abs(z_scores) > 0.5].sort_values(
                key=abs, ascending=False
            ).index.tolist()[:5]

            profiles.append(ClusterProfile(
                cluster_id=cluster_id,
                size=len(members),
                countries=members,
                mean_values=means.to_dict(),
                std_values=stds.to_dict(),
                characteristic_features=characteristic,
            ))

        return profiles


def cluster_countries(
    indicators: list[str],
    n_clusters: int = 4,
    year: int | None = None,
    method: str = "kmeans",
) -> ClusterResult:
    """
    Cluster countries based on multiple indicators.

    Args:
        indicators: List of indicator codes.
        n_clusters: Number of clusters.
        year: Year to use.
        method: 'kmeans' or 'hierarchical'.

    Returns:
        ClusterResult object.
    """
    clusterer = CountryClusterer()
    clusterer.prepare_data(indicators, year)

    if method == "kmeans":
        return clusterer.kmeans(n_clusters)
    else:
        return clusterer.hierarchical(n_clusters)


def segment_by_development(
    year: int | None = None,
    n_segments: int = 4,
) -> ClusterResult:
    """
    Segment countries by development level using key indicators.

    Args:
        year: Year to use.
        n_segments: Number of segments.

    Returns:
        ClusterResult with development segments.
    """
    development_indicators = [
        "NY.GDP.PCAP.CD",      # GDP per capita
        "SP.DYN.LE00.IN",      # Life expectancy
        "SE.ADT.LITR.ZS",      # Adult literacy
        "SP.URB.TOTL.IN.ZS",   # Urbanization
        "SL.UEM.TOTL.ZS",      # Unemployment
    ]

    return cluster_countries(
        indicators=development_indicators,
        n_clusters=n_segments,
        year=year,
    )


def segment_by_economy(
    year: int | None = None,
    n_segments: int = 4,
) -> ClusterResult:
    """
    Segment countries by economic characteristics.

    Args:
        year: Year to use.
        n_segments: Number of segments.

    Returns:
        ClusterResult with economic segments.
    """
    economic_indicators = [
        "NY.GDP.PCAP.CD",          # GDP per capita
        "NY.GDP.MKTP.KD.ZG",       # GDP growth
        "FP.CPI.TOTL.ZG",          # Inflation
        "NE.TRD.GNFS.ZS",          # Trade openness
        "BX.KLT.DINV.WD.GD.ZS",    # FDI
    ]

    return cluster_countries(
        indicators=economic_indicators,
        n_clusters=n_segments,
        year=year,
    )


def find_similar_countries(
    country: str,
    indicators: list[str],
    year: int | None = None,
    n_similar: int = 5,
) -> pd.DataFrame:
    """
    Find countries most similar to a reference country.

    Args:
        country: Reference country code.
        indicators: Indicators to use for comparison.
        year: Year to use.
        n_similar: Number of similar countries to return.

    Returns:
        DataFrame with similar countries and distances.
    """
    clusterer = CountryClusterer()
    clusterer.prepare_data(indicators, year)

    if country not in clusterer.country_names:
        raise ValueError(f"Country {country} not in data")

    # Get reference country index
    ref_idx = clusterer.country_names.index(country)
    ref_vector = clusterer.scaled_data[ref_idx]

    # Calculate distances to all other countries
    distances = []
    for i, c in enumerate(clusterer.country_names):
        if c != country:
            dist = np.linalg.norm(clusterer.scaled_data[i] - ref_vector)
            distances.append({"country": c, "distance": dist})

    # Sort and return top n
    df = pd.DataFrame(distances).sort_values("distance")
    df["similarity"] = 1 / (1 + df["distance"])  # Convert to similarity score

    return df.head(n_similar)
