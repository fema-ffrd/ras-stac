class RasStacInputs:
    """
    A class to generate input json files in the context of RAS STAC item generation.
    """

    def __init__(self, source_bucket_name: str, stac_bucket_name: str, **kwargs):
        self.source_bucket_name = source_bucket_name
        self.stac_bucket_name = stac_bucket_name

    def geometry_item(
        self,
        geom_hdf: str,
        new_item_s3_key: str,
        topo_assets: list,
        lulc_assets: list,
        mannings_assets: list,
        other_assets: list,
    ) -> dict:
        """
        Expects keys: "prefix/file.ext"
        """
        topo_assets = [f"s3://{self.source_bucket_name}/{a}" for a in topo_assets]
        lulc_assets = [f"s3://{self.source_bucket_name}/{a}" for a in lulc_assets]
        mannings_assets = [
            f"s3://{self.source_bucket_name}/{a}" for a in mannings_assets
        ]
        other_assets = [f"s3://{self.source_bucket_name}/{a}" for a in other_assets]

        return {
            "geom_hdf": f"s3://{self.source_bucket_name}/{geom_hdf}",
            "new_item_s3_key": f"s3://{self.stac_bucket_name}/{new_item_s3_key}",
            "topo_assets": topo_assets,
            "mannings_assets": mannings_assets,
            "lulc_assets": lulc_assets,
            "other_assets": other_assets,
        }

    def plan_item(
        self,
        plan_hdf: str,
        new_plan_item_s3_key: str,
        geom_item_s3_key: str,
        sim_id: str,
        item_props: dict,
        ras_assets: list,
    ) -> dict:
        """
        Expects keys: "prefix/file.ext"
        """
        ras_assets = [f"s3://{self.source_bucket_name}/{a}" for a in ras_assets]

        return {
            "plan_hdf": f"s3://{self.source_bucket_name}/{plan_hdf}",
            "new_plan_item_s3_key": f"s3://{self.stac_bucket_name}/{new_plan_item_s3_key}",
            "geom_item_s3_key": f"s3://{self.stac_bucket_name}/{geom_item_s3_key}",
            "sim_id": sim_id,
            "item_props": item_props,
            "ras_assets": ras_assets,
        }

    def dg_item(
        self,
        plan_dg: str,
        new_dg_item_s3_key: str,
        plan_item_s3_key: str,
        dg_id: str,
        item_props: dict,
    ) -> dict:
        """
        Expects keys: "prefix/file.ext"
        """

        return {
            "plan_dg": f"s3://{self.source_bucket_name}/{plan_dg}",
            "new_dg_item_s3_key": f"s3://{self.stac_bucket_name}/{new_dg_item_s3_key}",
            "plan_item_s3_key": f"s3://{self.stac_bucket_name}/{plan_item_s3_key}",
            "dg_id": dg_id,
            "item_props": item_props,
        }
