using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureStorageGetFilesApp
{
    class Program
    {
        // 取得するアカウント名(環境に合わせて書き換える)
        private const string accountName = "<your storage account name>";
        // 接続するアカウントのアクセスキー(環境に合わせて書き換える)
        private const string accessKey = "<your storage accessKey>";

        // 取得するBlob名(取得対象に合わせて書き換える)
        private const string blobName = "<blob name>";

        // 取得するBlobパス(取得対象に合わせて書き換える)
        // 必要ない場合は、「""」設定する　※全件取得
        private const string accessPrefix = "";

        // 取得するディレクトリを指定(取得対象に合わせて書き換える)
        // 必要ない場合は、「""」設定する
        // カンマ区切りで複数指定可
        private static List<string> targetList = new List<string>()
        {
            "",
        };

        /// <summary>
        /// ログインユーザ名取得
        /// </summary>
        /// <returns></returns>
        private static string GetLocalUserName()
        {
            return Environment.UserName;
        }

        /// <summary>
        /// 主処理
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        static async Task Main(string[] args)
        {
            Console.WriteLine("処理開始");

            // ストレージアカウントに接続
            var credential = new StorageCredentials(accountName, accessKey);
            var storageAccount = new CloudStorageAccount(credential, true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Blobコンテナを参照
            CloudBlobContainer container = blobClient.GetContainerReference(blobName);

            // 一時作業用Blobコンテナを参照
            CloudBlobContainer tmpcontainer = blobClient.GetContainerReference("tmp");


            // 一時作業用Blobコンテナのファイルダウンロードを行うためSAS接続を行う
            var sasContraints = new SharedAccessBlobPolicy();
            sasContraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(0);
            sasContraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(+1);
            sasContraints.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;
            var sasTmpContainerToken = tmpcontainer.GetSharedAccessSignature(sasContraints);
            var sasContainerToken = container.GetSharedAccessSignature(sasContraints);

            try
            {

                // 一時作業用BloBストレージが存在しない場合作成する
                await tmpcontainer.CreateIfNotExistsAsync();

                foreach (var target in targetList)
                {

                    // Blobコンテンツを取得して一時作業用Blobストレージにアップロードする
                    await CreateZipAsync(container, blobClient, tmpcontainer, sasContainerToken,target);

                    // 一時作業用Blobストレージ内のファイルをローカルにダウンロードする
                    await DownloadFilesAsync(tmpcontainer, blobClient, sasTmpContainerToken);
                }

                // 一時作業用Blobストレージが存在する場合削除する
                await tmpcontainer.DeleteIfExistsAsync();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }

            Console.WriteLine("処理終了");
        }

        /// <summary>
        /// コンテナ内に格納されているファイル一覧を取得してZip化する
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobClient"></param>
        /// <param name="tmpcontainer"></param>
        /// <param name="sasToken"></param>
        /// <param name="target"></param>
        /// <returns></returns>
        private static async Task CreateZipAsync(CloudBlobContainer container, CloudBlobClient blobClient, CloudBlobContainer tmpcontainer, string sasToken, string target)
        {
            Console.WriteLine($"Zip作成アップロード処理開始：\t{DateTime.Now}\t{target}");

            var tasks = new List<Task>();
            int getFilePathCount = 0;

            BlobContinuationToken blobContinuationToken = null;              // リスト操作の継続トークン
            bool useFlatBlobListing = true;                                  // 再帰的に取得する
            string prefix = accessPrefix + target;                           // 取得対象
            BlobListingDetails blobListingDetails = BlobListingDetails.None; // 取得オプション
            int maxBlobsPerRequest = 1000;                                   // 取得する件数

            // セマフォ設定
            int max_outstanding = 100;
            int completed_count = 0;
            SemaphoreSlim sem = new SemaphoreSlim(max_outstanding, max_outstanding);

            try
            {

                do
                {
                    // 指定したコンテナ内ファイルのパスを取得する
                    var results = await container.ListBlobsSegmentedAsync(prefix, useFlatBlobListing, blobListingDetails, maxBlobsPerRequest, blobContinuationToken, null, null);
                    blobContinuationToken = results.ContinuationToken;
                    getFilePathCount = getFilePathCount + results.Results.Count();
                    foreach (IListBlobItem item in results.Results)
                    {
                        await sem.WaitAsync();
                        // Zipファイルを作成して一時Blobストレージにアップロードを行う
                        tasks.Add((CreateZip(container, blobClient, tmpcontainer, item.Uri.AbsolutePath, item.Uri.AbsoluteUri + sasToken)).ContinueWith((t) =>
                        {
                            if (t.Exception != null)
                            {
                                foreach (var ex in t.Exception.InnerExceptions)
                                {
                                    throw ex;
                                }
                            }

                            sem.Release();
                            Interlocked.Increment(ref completed_count);
                        }));
                    }
                } while (blobContinuationToken != null);

                await Task.WhenAll(tasks);
                Console.WriteLine($"Zip作成アップロード完了件数：\t{completed_count}/{getFilePathCount}");
            }
            catch (Exception e)
            {
                throw;
            }

            Console.WriteLine($"Zip作成アップロード処理終了：\t{DateTime.Now}\t{target}");
        }

        /// <summary>
        /// 指定したBlobコンテンツをZip化して一時作業用Blobストレージに格納する
        /// </summary>
        /// <param name="container"></param>
        /// <param name="tmpcontainer"></param>
        /// <param name="uripath"></param>
        /// <param name="url"></param>
        /// <returns></returns>
        private static async Task CreateZip(CloudBlobContainer container, CloudBlobClient blobClient, CloudBlobContainer tmpcontainer,string uripath,string url)
        {
            // Blob取得オプション
            BlobRequestOptions options = new BlobRequestOptions
            {
                ParallelOperationThreadCount = 8,      // 同時にアップロードできるブロックの数
                DisableContentMD5Validation = true,    // BLOBのダウンロード時にMD5検証を無効にする
                StoreBlobContentMD5 = false            // アップロードするときにMD5ハッシュを計算して保存しない
            };

            // Uriからzipファイル作成のための構成情報を取得する
            string uri = uripath.Replace("/" + blobName + "/", "");
            var extension = Path.GetExtension(uri);
            var splitTmp = uripath.Split("/");
            var fileName = uripath.Split("/").Last().Replace(extension, ".zip");
            var directoryPath = "";
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < splitTmp.Length - 1; i++)
            {
                sb.AppendFormat("{0}_",splitTmp[i]);
                directoryPath = sb.ToString();
            }
            var zipFileName = directoryPath + fileName;

            try
            {

                // AzureBlobStorageのファイルを参照
                var Blob = await blobClient.GetBlobReferenceFromServerAsync(new Uri(url));
                var isExist = await Blob.ExistsAsync();
                if (!isExist)
                {
                    Console.WriteLine($"ファイル存在なし：\t{DateTime.Now}\t{zipFileName}");
                }
                else
                {
                    // AzureBlobStorageのファイルを参照
                    CloudBlob cloudBlob = container.GetBlobReference(uri);

                    // データサイズを取得するためコンテナの属性を情報を取得
                    await cloudBlob.FetchAttributesAsync();

                    // データを読み込むバイト配列の初期化。
                    long fileByteLength = cloudBlob.Properties.Length;
                    byte[] fileBytes = new byte[fileByteLength];
                    
                    // データをバイト配列に読み込む。
                    await cloudBlob.DownloadToByteArrayAsync(fileBytes, 0);

                    // ZIPファイル作成(DotNetZip（Ionic Zip Library）を使用)。
                    Ionic.Zip.ZipFile zip = new Ionic.Zip.ZipFile();
                    zip.CompressionLevel = Ionic.Zlib.CompressionLevel.BestCompression;
                    zip.AlternateEncoding = System.Text.Encoding.GetEncoding("UTF-8");
                    zip.AlternateEncodingUsage = Ionic.Zip.ZipOption.Always;

                    // バイト配列をZIPに書き込む。
                    zip.AddEntry(uripath, fileBytes);

                    // ストリームにZIPを書き込む。
                    using (var zipStream = new MemoryStream())
                    {
                        zip.Save(zipStream);
                        zipStream.Seek(0, SeekOrigin.Begin);

                        // ストリームにしたZIPファイルを、BLOBへ書き込み。
                        CloudBlockBlob blobRef = tmpcontainer.GetBlockBlobReference(zipFileName);
                        // アップロード時のBlobサイズを100MBに設定
                        blobRef.StreamWriteSizeInBytes = 100 * 1024 * 1024;
                        // 一時作業用Blobストレージにアップロードを行う
                        await blobRef.UploadFromStreamAsync(zipStream, null, options, null);
                    }

                    Console.WriteLine($"アップロード完了：\t{DateTime.Now}\t{zipFileName}");
                }
            }
            catch (Exception e)
            {
                throw;
            }
        }

        /// <summary>
        /// コンテナ内に格納されているファイル一覧を取得してダウンロードする
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobClient"></param>
        /// <param name="sasToken"></param>
        /// <returns></returns>
        private static async Task DownloadFilesAsync(CloudBlobContainer container, CloudBlobClient blobClient, string sasToken)
        {
            Console.WriteLine($"ダウンロード処理開始：\t{DateTime.Now}");

            var tasks = new List<Task>();
            int getFilePathCount = 0;

            BlobContinuationToken blobContinuationToken = null;              // リスト操作の継続トークン
            bool useFlatBlobListing = true;                                  // 再帰的に取得する
            BlobListingDetails blobListingDetails = BlobListingDetails.None; // 取得オプション
            int maxBlobsPerRequest = 1000;                                   // 取得する件数

            // セマフォ設定
            int max_outstanding = 100;
            int completed_count = 0;
            SemaphoreSlim sem = new SemaphoreSlim(max_outstanding, max_outstanding);

            try
            {
                do
                {
                    // 一時作業用BloBコンテナ内のファイルパスを取得する
                    var results = await container.ListBlobsSegmentedAsync(null, useFlatBlobListing, blobListingDetails, maxBlobsPerRequest, blobContinuationToken, null, null);
                    blobContinuationToken = results.ContinuationToken;
                    getFilePathCount = getFilePathCount + results.Results.Count();
                    foreach (IListBlobItem item in results.Results)
                    {
                        await sem.WaitAsync();
                        // ファイルのダウンロードを行う
                        tasks.Add((DownloadFile(container, blobClient, item.Uri.AbsoluteUri + sasToken)).ContinueWith((t) =>
                        {
                            if (t.Exception != null)
                            {
                                foreach (var ex in t.Exception.InnerExceptions)
                                {
                                    throw ex;
                                }
                            }

                            sem.Release();
                            Interlocked.Increment(ref completed_count);

                        }));
                    }

                } while (blobContinuationToken != null);

                await Task.WhenAll(tasks);
                Console.WriteLine($"ダウンロード完了件数：\t{completed_count}/{getFilePathCount}");
            }
            catch (Exception e)
            {
                throw;
            }

            Console.WriteLine($"ダウンロード処理終了：\t{DateTime.Now}");
        }

        /// <summary>
        /// 取得したファイルをローカルフォルダにダウンロードする
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobClient"></param>
        /// <param name="url"></param>
        /// <returns></returns>
        private static async Task DownloadFile(CloudBlobContainer container, CloudBlobClient blobClient , string url)
        {
            // Blob取得オプション
            BlobRequestOptions options = new BlobRequestOptions
            {
                DisableContentMD5Validation = true,   // BLOBのダウンロード時にMD5検証を無効にする
                StoreBlobContentMD5 = false           // アップロードするときにMD5ハッシュを計算して保存しない
            };

            // Uriからローカルディレクトリパス作成のための構成情報を取得する
            var userName = GetLocalUserName();
            var downlodsPath = $@"C:/Users/{ userName}/Downloads/{blobName}/";
            var splitTmp = url.Split(new char[] { '/', '?' });
            var fileName = splitTmp[url.Split(new char[] { '/', '?' }).Length - 2];

            // 取得するStoragePathと同じディレクトリ構造が存在しない場合は作成する
            if (!Directory.Exists(downlodsPath))
            {
                Directory.CreateDirectory(downlodsPath);
            }

            try
            {

                // AzureBlobStorageのファイルを参照
                var Blob = await blobClient.GetBlobReferenceFromServerAsync(new Uri(url));
                var isExist = await Blob.ExistsAsync();
                if (isExist)
                {
                    // ローカルにダウンロードファイルと同じ名称のファイルが存在する場合は削除する
                    if (File.Exists(downlodsPath + fileName))
                    {
                        File.Delete(downlodsPath + fileName);
                    }

                    // AzureBlobStorageのファイルをダウンロード
                    await Blob.DownloadToFileAsync(downlodsPath + fileName, System.IO.FileMode.CreateNew, null, options, null);
                    // AzureBlobStorageのダウンロードしたファイルを削除する
                    await Blob.DeleteAsync();
                    Console.WriteLine($"ダウンロード完了：\t{DateTime.Now}\t{downlodsPath + fileName}");
                }
                else
                {
                    Console.WriteLine($"ファイル存在なし：\t{DateTime.Now}\t{url}");
                }

            }
            catch (Exception e)
            {
                throw;

            }
        }
    }
}
