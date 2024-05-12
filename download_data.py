import os
import wget
import argparse



if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--data", help="Path to the data folder")

    args = argparser.parse_args()
    
    if not args.data.endswith('/'):
        args.data = args.data + '/'
    # create a folder to store the data
    if not os.path.isdir(args.data):
        os.mkdir(args.data)

    repositories =  ['https://zenodo.org/records/5294965/files/ansible__ansible__2020-06-11_06-06-22.zip?download=1', 'https://zenodo.org/records/5294965/files/openshift__origin__2020-06-12_19-40-32.zip?download=1', 'https://zenodo.org/records/5294965/files/tensorflow__tensorflow__2020-06-13_06-54-28.zip?download=1']

    for repo in repositories:
        print(f'Downloading {repo}...')
        wget.download(repo, out=f'{args.data}data.zip')
        # unzip the downloaded data
        os.system(f'unzip {args.data}data.zip -d {args.data}')
        # remove the zip file
        os.remove(f'{args.data}data.zip')