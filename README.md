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
This is the an example for class object to use for regular application operations,
```code
public class User
{
    [BinarySize(64)]
    public string UserName { get; set; } = string.Empty;

    [BinarySize(128)]
    public string EmailId { get; set; } = string.Empty;
    public bool IsActive { get; set; }

    [BinarySize(88)]
    public string Password { get; set; } = string.Empty;

    /*    [BinarySize(128)]
        public byte[]? PasswordHash { get; set; }
    
        [BinarySize(128)]
        public byte[]? PasswordSalt { get; set; }
        */
}

```
We use FastMapper<>.MapToStruct() and FastMapper<>.MapToClass() to convert between the database record and the class objects. 
Here are two examples to setup a record for database, that is actually getting saved,
1. Sequential Struct 
```code

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct UserRecord
{
    public fixed byte UserName[64];
    public fixed byte EmailId[64];
    public byte IsActive;
    public fixed byte Password[88];
    /* TODO: fixing this is taking too much time, we use strings for now
    public fixed byte PasswordHash[128];
    public fixed byte PasswordSalt[128];
    */
}

```

2. Explicit Struct (Recommended)
```code
[StructLayout(LayoutKind.Explicit, Size = 1048)]
public unsafe struct JobRegisterRecord
{
    // Offset 0: Date (long = 8 bytes)
    [FieldOffset(0)]
    public fixed byte Date[8];

    // Offset 8: Start of String Blocks
    [FieldOffset(8)]
    public fixed byte Particulars[128];

    [FieldOffset(136)] // 8 + 128
    public fixed byte Entity[64];

    [FieldOffset(200)] // 136 + 64
    public fixed byte EntityAddress[256];

    [FieldOffset(456)] // 200 + 256
    public fixed byte Consignee[64];

    [FieldOffset(520)] // 456 + 64
    public fixed byte ConsigneeAddress[256];

    [FieldOffset(776)] // 520 + 256
    public fixed byte SomeField1[32];

    [FieldOffset(808)] // 776 + 32
    public fixed byte SomeField2[32];

    [FieldOffset(840)] // 808 + 32
    public fixed byte SomeField3[32];

    [FieldOffset(872)] // 840 + 32
    public fixed byte SomeField4[32];

    [FieldOffset(904)] // 872 + 32
    public fixed byte SomeField5[32];

    [FieldOffset(936)] // 904 + 32
    public fixed byte SomeField6[64];

    [FieldOffset(1000)] // 936 + 64
    public fixed byte SomeField7[32];

    // Offset 1032: Amount (decimal = 16 bytes)
    // 1032 is a multiple of 8, so this is perfectly aligned!
    [FieldOffset(1032)]
    public decimal Amount;
}

```
