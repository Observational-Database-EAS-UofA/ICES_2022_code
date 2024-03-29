from read import read_ICES


def read_bottle():
    data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_Bottle_to_2022/01110af6-4732-41d1-b272-f538e60d49c0_trim.csv'
    save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/ncfiles_raw'
    read_ICES(data_path, save_path, 'bot')


def read_ctd():
    data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_CTD_to_2022/027054ba-3719-449b-ae1f-b1d1b27959b1.txt'
    save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/ncfiles_raw'
    read_ICES(data_path, save_path, 'ctd')


def read_xbt():
    data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/original_data/ICESData_XBT_to_2022/ae6db793-7fa6-4eb8-a860-cc4724c8068d.txt'
    save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/ICES_2022/ncfiles_raw'
    read_ICES(data_path, save_path, 'xbt')


if __name__ == "__main__":
    read_xbt()
    read_ctd()
    read_bottle()
