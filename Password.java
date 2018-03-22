import java.util.Scanner;
class Password
{
public static void main (String []args)
{
System.out.println("Enter the name: ");
Scanner ss=new Scanner(System.in);
String s=ss.next();
System.out.println("Enter the number: ");
Scanner sc=new Scanner(System.in);
int n=sc.nextInt();
char value[]=s.toCharArray(); 
System.out.print(value[0]);
int sum=0,m;
while(n>0)
{
m=n%10;
sum=sum+m;
n=n/10;
}
System.out.print(sum);
char a[]={'&','*','-','+','@','$','!','%','#','='};
if(sum<=9)
{
System.out.print(a[sum]);
}
int flag=1,j;
for(j=value.length-1;j<=0;j--)
{
flag=1;
}
if(flag==1)
{
System.out.print(value[j]);
}
}
}