from .data_loader import DataLoader

class DMSPLoader(DataLoader):
    def __init__(self, config: dict, global_config: dict):
        super().__init__(config, global_config)

    def apply_specific_filters(self):
        self.df = self.df[self.df['MaSanPham'].notnull()]
        self.df = self.df.drop_duplicates(subset=['MaSanPham'])
        
        return self.df

    def process_data(self):
        """
        Overrides the base process_data to include specific filtering after transformations.
        """
        super().process_data() # Call base class processing first
        if not self.df.empty:
            self.apply_specific_filters()
        return self.df
