import argparse
import re
from pathlib import Path


def arguments():
    parser = argparse.ArgumentParser(description="")
    default_out = str(Path.cwd())
    parser.add_argument("--in_dir", required=True, help="Input directory")
    parser.add_argument("--out", default=default_out, help=f"Output directory [default: {default_out}]")
    return parser.parse_args()


def main():
    args = arguments()
    input_dir = Path(args.in_dir)
    pics = [pic for pic in input_dir.iterdir() if re.search("jepg|jpg|gif|png", str(pic).lower())]
    pics.sort()
    with open(Path(args.out) / "extra_pics.html", "w") as pics_file:
        for pic_file in pics:
            pic_file_str = str(pic_file)
            pic_name = pic_file_str[pic_file_str.rfind("/") + 1:pic_file_str.rfind(".")]
            pics_file.write(f'<img src="{pic_file}" style="width:400px" alt="{pic_name}"><br>\n')


if __name__ == "__main__":
    main()
