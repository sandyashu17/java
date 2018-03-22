import java.util.Scanner;
class EligibleToVote
{
public static void main(String []args)
{
int age=0, total=0;
System.out.println("Enter the number of students");
Scanner ss=new Scanner(System.in);
int n=ss.nextInt();
Scanner sc=new Scanner(System.in);
Scanner si=new Scanner(System.in);
for(int i=1;i<=n;)
{
System.out.println("Enter the name:");
String name=sc.next();
System.out.println("Enter the Age:");
age=si.nextInt();
if(age>=18)
{
total=total+1;
}
i++;
}
System.out.println(total+" number of students are eligible to vote");
}
}