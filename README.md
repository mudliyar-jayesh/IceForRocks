# IceForRocks.NET
## A file-based database library. No Bloat, Just Speed!

### Why?
I just have a project, that solely runs on file inputs from the user, and runs locally on the user's server. 
Deploying a whole database there, just for files seemed too much. 
Also, Converting the file data then breaking them down to database compatible structres was too much. 

But the most important reason is, that I can! 😂

### Not Released!
I just started this project, a local nuget store on my machine has the version 1. 

I have a LOT, i mean a LOT of improvements before this actually becomes production ready. 
I am using this in a project because it works for the purpose I need it to work for. 

Soon I will complete the official version 1.


### What Now? 
Explore the code, it ain't that much. but Suggestions are ALWAYS WELCOME! 🫡🤝

### How to handle Models? 
The data storing will be handled by a struct, following is an example 
```code

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct JobRow
{
    public int JobNoId;             
    
    // --- Date (Stored as YYYYMMDD for easy sorting/filtering) ---
    public int DateEncoded;         

    public double LedgerAmount;     

    public int VoucherTypeId;       
    public int BranchId;            
    public int LedgerNameId;        
    public int ExpenseTypeId;
    public int ParticularsId;

    public long VoucherNoOffset;    
    public int VoucherNoLength;     
    
}


```
Now, I did not have a sample code, so, this is how I am using the IceStore in a project I have 
```code
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using CHABusiness.Models;
using CHABusiness.Records;
using CHABusiness.Utility;
using IceForRocks.CoreV2;

namespace CHABusiness.Repositories;



public class JobRepository :  IDisposable
{
    private readonly IceStore<JobRow> _store;

    public JobRepository(string dbRoot)
    {
        _store = new IceStore<JobRow>(dbRoot, "JobRegister");
    }
    

    public void Ingest(string[] parts)
    {
        /*
         * Column order
         * 0 - "Voucher No."
         * 1 - "Date"
         * 2 - "Voucher Type"
         * 3 - "Branch"
         * 4 - "Party Name"
         * 5 - "Amount"
         * 6 - "Job No."
         * 7 - "Ledger Name"
         * 8 - "Ledger Amount"
         * 9 - "Expense Type",
         */
        var row = new JobRow();
        (row.VoucherNoOffset, row.VoucherNoLength) = _store.WriteSlush(SanitizeCsvValue(parts[0]));
        DateTime safeDate = DataTypeHelper.ParseSafe(SanitizeCsvValue(parts[1]));
        row.DateEncoded = ParseDateToInt(safeDate.ToString("yyyy-MM-dd"));
        row.VoucherTypeId = _store.AddToMap("VoucherType", SanitizeCsvValue(parts[2]));
        row.BranchId = _store.AddToMap("Branch", SanitizeCsvValue(parts[3]));
        row.ParticularsId = _store.AddToMap("Particulars", SanitizeCsvValue(parts[4]));
        row.JobNoId = _store.AddToMap("JobNo", SanitizeCsvValue(parts[6]));
        row.LedgerNameId = _store.AddToMap("Ledger", SanitizeCsvValue(parts[7]));
        row.LedgerAmount = double.TryParse(SanitizeCsvValue(parts[8]), out var amt) ? amt : 0;
        row.ExpenseTypeId = _store.AddToMap("ExpenseType", SanitizeCsvValue(parts[9]));
        _store.Insert(row);
    }

    private string SanitizeCsvValue(string original)
    {
        if  (string.IsNullOrEmpty(original))
        {
            return string.Empty;
        }

        if (original.StartsWith('"'))
        {
            original = original.Trim('"');
        }
        return original;
    }

    public void Commit()
    {
        _store.CommitBlock();
        _store.CommitSlush();
    }


    public List<string> GetBranches()
    {
        return _store.GetMap("Branch").GetValues();
    }

 
    public (int Count, List<JobSummary> Data) GetJobSummary(JobSummaryParams filters)
    {
        int incomeId = _store.GetId("ExpenseType", "Income");
        int expenseId = _store.GetId("ExpenseType", "Expense");

        var buckets = new Dictionary<(int JobId, int LedgerId, int particularId), JobSummaryAccumulator>();

        var branchIds = new List<int>();
        if (filters.BranchNames.Any())
        {
            branchIds = filters.BranchNames.Select(x => _store.GetId("Branch", x)).ToList();
        }
        string searchKey = filters.SearchType switch
        {
            "ptry" => "Ledger",
            "prtc" => "Particulars",
            _ => "JobNo"
        };
        var searchIds = new List<int>();
        if (!string.IsNullOrEmpty(filters.SearchValue))
        {
            searchIds = _store.GetMap(searchKey).GetValues().Where(x => x.Contains(filters.SearchValue))
                .Select(y => _store.GetId(searchKey, y))
                .ToList();
        }

        _store.Walk((JobRow row, int index) =>
        {
            if (branchIds.Any() && !branchIds.Contains(row.BranchId))
            {
                return;
            }

            if (searchIds.Any())
            {
                bool mismatch = searchKey switch
                {
                    "JobNo" => !searchIds.Contains(row.JobNoId),
                    "Ledger" => !searchIds.Contains(row.LedgerNameId),
                    "Particulars" => !searchIds.Contains(row.ParticularsId),
                    _ => false,
                };
                if (mismatch)
                {
                    return;
                }
            }
            
            var key = (row.JobNoId, row.LedgerNameId, row.ParticularsId);
            if (!buckets.TryGetValue(key, out var acc))
            {
                acc = new JobSummaryAccumulator();
                acc.JobId = row.JobNoId;
                acc.LedgerId = row.LedgerNameId;
                acc.ParticularsId = row.ParticularsId;
                acc.FirstRowId = index;
                buckets[key] = acc;
            }
            acc.FoundIndices.Add(index);
            if (row.ExpenseTypeId == incomeId)
            {
                acc.TotalIncome += Math.Abs(row.LedgerAmount);
            }
            else if (row.ExpenseTypeId == expenseId)
            {
                acc.TotalExpense += Math.Abs(row.LedgerAmount);
            }
        });

        IEnumerable<JobSummaryAccumulator> filtered = filters.CashFilter switch
        {
            "inc_zero" => buckets.Values.Where(x => x.TotalIncome == 0),
            "exp_zero" => buckets.Values.Where(x => x.TotalExpense == 0),
            "exp_gt"   => buckets.Values.Where(x => x.TotalExpense > x.TotalIncome),
            "inc_gt"   => buckets.Values.Where(x => x.TotalIncome > x.TotalExpense),
            _          => buckets.Values
        };

        int count = filtered.Count();

        return (count,filtered
            .Skip(filters.Skip)
            .Take(filters.Take)
            .Select(acc => ThawSummary(acc))
            .OrderBy(acc => acc.JobNo)
            .ToList());
    }

    private JobSummary ThawSummary(JobSummaryAccumulator acc)
    {
        var row = _store.GetRowAt(acc.FirstRowId);
        return new JobSummary
        {
            JobNo = _store.GetValue("JobNo", acc.JobId),
            LedgerName = _store.GetValue("Ledger", acc.LedgerId),
            JobDate = DateIntToString(row.DateEncoded),
            PartyName = _store.GetValue("Particulars", row.ParticularsId),
            TotalIncome = acc.TotalIncome,
            TotalExpense = acc.TotalExpense,
            Balance = (decimal)(acc.TotalIncome - acc.TotalExpense),
            RegisterIndices = acc.FoundIndices.ToList(),
        };
    }

    private JobRegister ThawToRegister(int index)
    {
        var row = _store.GetRowAt(index);
        return new JobRegister
        {
            JobNo = row.JobNoId.ToString(),
            LedgerAmount = row.LedgerAmount.ToString(CultureInfo.InvariantCulture),
            Date = DateHelper.ParseSafe(DataTypeHelper.DateIntToString(row.DateEncoded)),
            VoucherNo = _store.ReadSlush(row.VoucherNoOffset, row.VoucherNoLength),
            VoucherType = _store.GetValue("VoucherType", row.VoucherTypeId),
            Branch = _store.GetValue("Branch", row.BranchId),
            LedgerName = _store.GetValue("Ledger", row.LedgerNameId),
            ExpenseType = _store.GetValue("ExpenseType", row.ExpenseTypeId)
        };
    }

    private int ParseDateToInt(string date) 
        => int.TryParse(date.Replace("-", ""), out var d) ? d : 0;

    private string DateIntToString(int date)
        => DateTime.TryParseExact(date.ToString(), "yyyyMMdd", null, DateTimeStyles.None, out var dt) 
            ? dt.ToString("dd-MM-yyyy") : date.ToString();

    public void Dispose()
    {
        _store.Dispose();
    }
}

internal class JobSummaryAccumulator
{
    public int FirstRowId;
    public int JobId;
    public int LedgerId;
    public int ParticularsId;
    public double TotalIncome;
    public double TotalExpense;
    public List<int> FoundIndices { get; set;  } = new List<int>();
}

```
Yes, this might be a hassle, but here is what the FUTURE SCOPE includes, a migration setup for source generation, 
here is how I plan to setup a record for migration, 
```code
[IceRecord] // My custom attribute
public partial class JobRegister
{
    [BinarySize(8)] // Automatically becomes 'fixed byte[8]' in the struct
    public DateTime Date { get; set; }

    [BinarySize(128)] 
    public string Particulars { get; set; }
}
```
